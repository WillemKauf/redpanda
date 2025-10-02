/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/worker.h"

#include "cloud_topics/level_one/compaction/logger.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "cloud_topics/level_one/compaction/sink.h"
#include "cloud_topics/level_one/compaction/source.h"
#include "cloud_topics/level_one/compaction/worker_manager.h"
#include "compaction/reducer.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "ssx/future-util.h"

#include <seastar/coroutine/as_future.hh>

namespace cloud_topics::l1 {

compaction_worker::compaction_worker(
  worker_manager* worker_manager,
  io* io,
  metastore* metastore,
  compaction_committer* committer)
  : _worker_manager(worker_manager)
  , _io(io)
  , _metastore(metastore)
  , _committer(committer) {}

ss::future<> compaction_worker::start() {
    start_bg_loop();
    co_return;
}

void compaction_worker::start_bg_loop() {
    _bg_fut = ssx::spawn_with_gate_then(_gate, [this]() { return bg_loop(); });
}

ss::future<> compaction_worker::bg_loop() {
    constexpr std::chrono::seconds poll_frequency(60);

    while (is_active()) {
        try {
            co_await _worker_cv.wait(poll_frequency);
        } catch (const ss::semaphore_timed_out&) {
            // Fall through
        }

        while (is_active()) {
            auto maybe_work = co_await try_acquire_work_from_manager();

            if (!maybe_work.has_value()) {
                break;
            }

            auto work = std::move(maybe_work).value();
            co_await compact_log(work);
        }
    }
}

ss::future<> compaction_worker::clear_bg_fut() {
    if (_bg_fut.has_value()) {
        auto bg_fut = std::exchange(_bg_fut, std::nullopt);
        co_await std::move(bg_fut).value();
    }
}

ss::future<> compaction_worker::compact_log(log_compaction_meta* log) {
    if (!is_active()) {
        co_return;
    }

    // If there was a concurrent race with a request to cancel/stop an inflight
    // compaction, early return after resetting state to `idle`.
    if (
      _job_state == compaction_job_state::soft_stop
      || _job_state == compaction_job_state::hard_stop) {
        _job_state = compaction_job_state::idle;
        co_return;
    }

    if (!log) {
        co_return;
    }

    if (!log->link.is_linked()) {
        co_return;
    }

    auto tidp = log->tidp;
    auto ntp = log->ntp;

    if (!log->info_and_ts.has_value()) {
        vlog(
          compaction_log.error,
          "Log {} in compaction process did not have metastore information "
          "set. Concurrency issue?",
          tidp);
        co_return;
    }

    vlog(compaction_log.info, "Compacting CTP {}", tidp);

    _job_state = compaction_job_state::running;

    // Very important to hold the log's gate in addition to our own (held by
    // backgrounded loop).
    auto holder = log->gate.hold();

    // Copy
    auto compaction_offsets = log->info_and_ts->info.offsets_response;

    // Lazy initialization of offset map.
    if (!_map) {
        co_await initialize_map();
    }

    auto dirty_range_intervals = compaction_offsets.dirty_ranges.to_vec();
    auto extents = compaction_offsets.extents;

    auto src = std::make_unique<compaction_source>(
      std::move(ntp),
      tidp,
      dirty_range_intervals,
      compaction_offsets.removable_tombstone_ranges,
      std::move(extents),
      _map.get(),
      _metastore,
      _io,
      _as,
      _job_state);
    auto sink = std::make_unique<compaction_sink>(
      tidp,
      dirty_range_intervals,
      compaction_offsets.removable_tombstone_ranges,
      _io,
      _committer);
    auto reducer = compaction::sliding_window_reducer(
      std::move(src), std::move(sink));

    auto compact_fut = co_await ss::coroutine::as_future(
      std::move(reducer).run());

    if (compact_fut.failed()) {
        auto eptr = compact_fut.get_exception();
        auto log_lvl = ssx::is_shutdown_exception(eptr) ? ss::log_level::debug
                                                        : ss::log_level::warn;
        vlogl(
          compaction_log,
          log_lvl,
          "Caught exception {} while compacting CTP {}.",
          eptr,
          tidp);
    }

    co_await finish_work(log);
    _job_state = compaction_job_state::idle;
}

ss::future<std::optional<log_compaction_meta*>>
compaction_worker::try_acquire_work_from_manager() {
    co_return co_await ss::smp::submit_to(
      worker_manager::worker_manager_shard,
      [this, shard = ss::this_shard_id()]() {
          return _worker_manager->try_acquire_work(shard);
      });
}

ss::future<> compaction_worker::finish_work(log_compaction_meta* log) {
    co_return co_await ss::smp::submit_to(
      worker_manager::worker_manager_shard,
      [this, log, shard = ss::this_shard_id()]() {
          _worker_manager->finish_work(log, shard);
      });
}

bool compaction_worker::is_active() const {
    return !_gate.is_closed() && !_as.abort_requested()
           && _worker_state == worker_state::active;
}

void compaction_worker::request_soft_stop() {
    _job_state = compaction_job_state::soft_stop;
}

void compaction_worker::request_hard_stop() {
    _job_state = compaction_job_state::hard_stop;
}

ss::future<> compaction_worker::pause_worker() {
    // If worker is `stopped`, we shouldn't be able to resume it. If it is
    // already `paused`, this is a no-op.
    if (_worker_state != worker_state::active) {
        co_return;
    }

    request_soft_stop();

    _worker_state = worker_state::paused;

    // Signal `_worker_cv` in case bg_loop is currently waiting.
    alert_worker();
    co_await clear_bg_fut();
}

ss::future<> compaction_worker::resume_worker() {
    // If worker is `stopped`, we shouldn't be able to resume it. If it is
    // already `active`, this is a no-op.
    if (_worker_state != worker_state::paused) {
        co_return;
    }

    // Wait till the existing background loop has finished, if it exists (though
    // it probably shouldn't?)
    co_await clear_bg_fut();

    // Set state back to active and start a new background loop.
    _worker_state = worker_state::active;
    start_bg_loop();
}

ss::future<> compaction_worker::stop() {
    request_hard_stop();
    _worker_state = worker_state::stopped;

    _as.request_abort();
    _worker_cv.broken();
    auto close_fut = _gate.close();

    co_await clear_bg_fut();

    co_await std::move(close_fut);
}

void compaction_worker::alert_worker() { _worker_cv.signal(); }

ss::future<> compaction_worker::initialize_map() {
    if (_map) {
        co_return;
    }

    // TODO: use memory group reservation.
    // auto compaction_mem_bytes = memory_groups().compaction_reserved_memory();
    auto compaction_mem_bytes
      = config::shard_local_cfg().storage_compaction_key_map_memory();
    auto compaction_map = std::make_unique<compaction::hash_key_offset_map>();
    co_await compaction_map->initialize(compaction_mem_bytes);
    _map = std::move(compaction_map);
}

} // namespace cloud_topics::l1
