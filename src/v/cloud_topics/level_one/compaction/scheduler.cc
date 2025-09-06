/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/scheduler.h"

#include "cloud_topics/level_one/compaction/logger.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "cloud_topics/level_one/compaction/scheduling_policies.h"
#include "config/configuration.h"
#include "ssx/future-util.h"

namespace cloud_topics::l1 {

compaction_scheduler::compaction_scheduler(
  std::unique_ptr<log_collector> log_collector,
  std::unique_ptr<scheduling_policy> policy)
  : _log_collector(std::move(log_collector))
  , _scheduling_policy(std::move(policy))
  , _executor(_as, _gate)
  , _compaction_interval(
      config::shard_local_cfg().log_compaction_interval_ms.bind()) {
    _compaction_interval.watch([this]() { _scheduling_loop_sem.signal(); });

    ssx::repeat_until_gate_closed_or_aborted(_gate, _as, [this] {
        return scheduling_loop().handle_exception(
          [](const std::exception_ptr& e) {
              auto log_level = ssx::is_shutdown_exception(e)
                                 ? ss::log_level::debug
                                 : ss::log_level::warn;
              vlogl(
                compact_log,
                log_level,
                "Encountered exception in main loop: {}",
                e);
          });
    });
}

bool compaction_scheduler::is_managed(const model::ntp& ntp) const {
    return _logs.contains(ntp);
}

void compaction_scheduler::manage_partition(const model::ntp& ntp) {
    vlog(compact_log.info, "Asked to manage compacted log: {}", ntp);
    auto [it, success] = _logs.insert(
      std::make_unique<log_compaction_meta>(ntp));
    _logs_list.push_back(*(*it));
    vassert(
      success, "Could not manage compacted log {} (concurrency issue?)", ntp);
}

ss::future<> compaction_scheduler::unmanage_partition(const model::ntp& ntp) {
    vlog(compact_log.info, "Asked to unmanage compacted log: {}", ntp);
    auto handle_opt = _logs.extract(ntp);
    if (!handle_opt) {
        co_return;
    }

    auto handle = std::move(handle_opt).value();

    auto close_fut = handle->gate.close();
    handle->state = compaction_state::stopped;

    co_await std::move(close_fut);
}

ss::future<> compaction_scheduler::scheduling_loop() {
    vlog(compact_log.debug, "Starting compaction scheduling loop");
    auto holder = _gate.hold();
    while (!_gate.is_closed && !_as.abort_requested()) {
        auto compaction_interval = _compaction_interval();
        try {
            co_await _scheduling_loop_sem.wait(
              _compaction_interval(),
              std::max(_scheduling_loop_sem.current(), size_t(1)));
        } catch (const ss::semaphore_timed_out&) {
            // Fall through
        }

        if (compaction_interval != _compaction_interval()) {
            // Cluster config was changed while waiting.
            continue;
        }

        co_await schedule_some();
    }
}

ss::future<> compaction_scheduler::schedule_some() {
    // auto should_compact_log = [](auto&& ntp) {
    //     auto needs_compact = ntp->needs_compaction();
    //     if (!needs_compact) {
    //         vlog(
    //           compact_log.trace,
    //           "{}: dirty ratio ({}) < min.cleanable.dirty.ratio ({}) and "
    //           "time since earliest dirty timestamp does not exceed "
    //           "max.compaction.lag.ms ({}), skipping compaction.",
    //           ntp->config().ntp(),
    //           ntp->dirty_ratio(),
    //           ntp->config().min_cleanable_dirty_ratio(),
    //           ntp->config().max_compaction_lag_ms());
    //     }
    //     return needs_compact;
    // };

    auto log_infos = []() { return chunked_vector<log_info>{}; }();
    // auto log_infos = co_await sample_logs();
    co_await _scheduling_policy->schedule_compactions(
      _executor, std::move(log_infos));
}

ss::future<> compaction_scheduler::stop() {
    vlog(compact_log.debug, "Stopping compaction scheduling loop");
    _as.request_abort();
    _scheduling_loop_sem.broken();
    auto close_fut = _gate.close();
    static constexpr size_t max_concurrent_close = 1024;

    co_await ss::max_concurrent_for_each(
      _logs.begin(), _logs.end(), max_concurrent_close, [](auto& log) {
          log->state = compaction_state::stopped;
          return log->gate.close();
      });

    _logs.clear();

    co_await std::move(close_fut);
    co_await _executor.stop();
}

} // namespace cloud_topics::l1
