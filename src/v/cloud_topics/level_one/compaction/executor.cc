/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/executor.h"

#include "cloud_topics/level_one/metastore/replicated_metastore.h"

namespace cloud_topics::l1 {

compaction_executor::compaction_executor(
  ss::sharded<file_io>* io,
  ss::sharded<replicated_metastore>* metastore,
  ss::sharded<compaction_committer>* committer)
  : _io(io)
  , _metastore(metastore)
  , _committer(committer) {}

ss::future<> compaction_executor::start() {
    co_await _workers.start(
      ss::sharded_parameter([this] { return &_io->local(); }),
      ss::sharded_parameter([this] { return &_metastore->local(); }),
      ss::sharded_parameter([this] { return &_committer->local(); }));

    populate_workers();
}

void compaction_executor::populate_workers() {
    for (worker_shard worker = 0; worker < ss::smp::count; ++worker) {
        auto [it, success] = _worker_meta.emplace(
          std::make_unique<worker_meta>(worker));
        _avail_workers.push_back(*(*it));
    }

    // Awake any waiters that may already exist.
    _cvar.broadcast();
}

ss::future<> compaction_executor::stop() { co_await _workers.stop(); }

ss::future<> compaction_executor::compact_log(log_info_and_meta log) {
    if (!log.meta->link.is_linked()) {
        co_return;
    }

    auto holder = log.meta->gate.hold();

    auto worker_fut = co_await ss::coroutine::as_future(
      get_available_worker(log.meta->tidp));
    if (worker_fut.failed()) {
        auto eptr = worker_fut.get_exception();
        auto log_lvl = ssx::is_shutdown_exception(eptr) ? ss::log_level::warn
                                                        : ss::log_level::debug;
        vlogl(
          compaction_log,
          log_lvl,
          "Caught exception {} while waiting for compaction worker.",
          eptr);
        co_return;
    }

    auto worker = worker_fut.get();
    co_await do_compact_log(worker, std::move(log));
}

ss::future<>
compaction_executor::request_stop_compaction(model::topic_id_partition tidp) {
    auto it = _inflight_tidp_to_shard.find(tidp);
    if (it == _inflight_tidp_to_shard.end()) {
        co_return;
    }

    auto shard = it->second;
    co_await _workers.invoke_on(shard, [](compaction_worker& worker) {
        return worker.request_hard_stop();
    });
}

ss::future<> compaction_executor::request_stop_workers() {
    _as.request_abort();
    _cvar.broken();
    co_await _workers.invoke_on_all(
      [](compaction_worker& worker) { worker.stop_worker(); });
}

compaction_executor::worker_meta&
compaction_executor::get_worker_meta(worker_shard worker) {
    auto it = _worker_meta.find(worker);

    if (it == _worker_meta.end()) {
        throw std::runtime_error("This should never happen.");
    }

    return *it->get();
}

ss::future<> compaction_executor::set_worker_state(
  worker_shard worker, worker_meta::state new_state) {
    if (worker >= _worker_meta.size()) {
        co_return;
    }

    auto& meta = get_worker_meta(worker);

    if (meta.current_state == new_state) {
        co_return;
    }

    meta.current_state = new_state;
    auto is_linked = meta.link.is_linked();
    switch (new_state) {
    case worker_meta::state::paused: {
        if (is_linked) {
            // Remove from `_avail_workers` pool.
            meta.link.unlink();
        } else {
            // Request that in the flight compaction be cancelled. The worker
            // won't be returned to the `_avail_workers` pool when `state ==
            // paused`.
            co_await _workers.invoke_on(worker, [](compaction_worker& worker) {
                worker.request_soft_stop();
            });
        }
        break;
    }
    case worker_meta::state::active: {
        if (!is_linked) {
            // If the worker is not currently in the pool, it is either
            // inflight, or in a correctly paused state without an entry in
            // `_avail_workers`. If it is inflight, allow the worker to
            // be returned to the `_avail_workers` pool when the compaction job
            // finishes. If it is not inflight, add it back to the
            // `_avail_workers` pool now and alert a waiter.
            auto is_inflight = meta.tidp.has_value();
            if (!is_inflight) {
                _avail_workers.push_back(meta);
                _cvar.signal();
            }
        }
    }
    }
}

ss::future<> compaction_executor::do_compact_log(
  worker_shard worker, log_info_and_meta log) {
    if (!log.meta->link.is_linked()) {
        co_return;
    }

    co_await _workers.invoke_on(
      worker,
      [this,
       ntp = log.meta->ntp,
       tidp = log.meta->tidp,
       offsets = std::move(log.info.offsets_response)](
        compaction_worker& worker) {
          return worker.compact(std::move(ntp), tidp, std::move(offsets), _as);
      });

    return_worker(worker, log.meta->tidp);
}

void compaction_executor::return_worker(
  worker_shard worker, const model::topic_id_partition& tidp) {
    _inflight_tidp_to_shard.erase(tidp);
    auto& meta = get_worker_meta(worker);
    meta.tidp.reset();
    if (meta.current_state != worker_meta::state::paused) {
        _avail_workers.push_back(meta);
        _cvar.signal();
    }
}

ss::future<compaction_executor::worker_shard>
compaction_executor::get_available_worker(
  const model::topic_id_partition& tidp) {
    while (!_as.abort_requested()) {
        if (!_avail_workers.empty()) {
            auto& worker = _avail_workers.front();
            if (worker.current_state == worker_meta::state::active) {
                auto shard = worker.shard;
                _inflight_tidp_to_shard.emplace(tidp, shard);
                worker.tidp = tidp;
                _avail_workers.pop_front();
                co_return shard;
            }
        }

        co_await ssx::with_timeout_abortable(
          _cvar.wait(), model::no_timeout, _as);
    }

    __builtin_unreachable();
}

} // namespace cloud_topics::l1
