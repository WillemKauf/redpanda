/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/level_one/compaction/logger.h"
#include "cloud_topics/level_one/compaction/worker.h"
#include "container/chunked_circular_buffer.h"
#include "model/timeout_clock.h"
#include "ssx/future-util.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/sharded.hh>

namespace cloud_topics::l1 {

class compaction_executor {
public:
    compaction_executor(ss::abort_source& as, ss::gate& gate)
      : _as(as)
      , _gate(gate) {
        for (worker_shard i = 0; i < ss::smp::count; ++i) {
            _avail_workers.emplace_back(i);
        }
    }

    ss::future<> compact_one(model::ntp ntp) {
        auto worker_fut = co_await ss::coroutine::as_future(
          get_available_worker());
        if (worker_fut.failed()) {
            auto eptr = worker_fut.get_exception();
            auto log_lvl = ssx::is_shutdown_exception(eptr)
                             ? ss::log_level::warn
                             : ss::log_level::debug;
            vlogl(
              compact_log,
              log_lvl,
              "Caught exception {} while waiting for compaction worker.",
              eptr);
            co_return;
        }

        auto worker = worker_fut.get();
        do_compact(worker, std::move(ntp));
    }

    ss::future<> start() { co_await _workers.start(); }

    ss::future<> stop() {
        _cvar.broken();
        co_await _workers.stop();
    }

private:
    using worker_shard = ss::shard_id;

    void do_compact(worker_shard shard, model::ntp ntp) {
        ssx::spawn_with_gate(
          log_meta_gate, [shard, ntp = std::move(ntp), this] {
              return _workers
                .invoke_on(
                  shard,
                  [this, ntp = std::move(ntp)](compaction_worker& worker) {
                      return worker.compact(std::move(ntp), _as);
                  })
                .finally([shard, this] {
                    _avail_workers.emplace_back(shard);
                    _cvar.signal();
                });
          });
    }

    ss::future<worker_shard> get_available_worker() {
        while (!_gate.is_closed() && !_as.abort_requested()) {
            if (!_avail_workers.empty()) {
                auto worker = _avail_workers.front();
                _avail_workers.pop_front();
                co_return worker;
            }

            co_await ssx::with_timeout_abortable(
              _cvar.wait(), model::no_timeout, _as);
        }

        __builtin_unreachable();
    }

    ss::abort_source& _as;
    ss::gate& _gate;

    ss::condition_variable _cvar;

    chunked_circular_buffer<worker_shard> _avail_workers;
    ss::sharded<compaction_worker> _workers;
};

} // namespace cloud_topics::l1
