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

#include "cloud_topics/level_one/compaction/executor.h"
#include "cloud_topics/level_one/compaction/log_collector.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "cloud_topics/level_one/compaction/scheduling_policies.h"
#include "cluster/partition.h"
#include "config/property.h"
#include "container/chunked_hash_map.h"
#include "container/intrusive_list_helpers.h"
#include "model/fundamental.h"
#include "ssx/semaphore.h"

namespace cloud_topics::l1 {

/*
 * Responsible for scheduling compaction for cloud topic partitions.
 */
class compaction_scheduler {
public:
    compaction_scheduler(
      log_collector_cluster_state, std::unique_ptr<scheduling_policy>);

    // Starts the contained `_log_collector`, `_executor`, and the backgrounded
    // scheduling loop.
    ss::future<> start();

    // Shuts down concurrency primitives, thereby stopping the backgrounded
    // scheduling loop, stops the `_log_collector`, requests inflight compaction
    // jobs in the `_executor` be stopped, drains the managed partition log
    // list, and finally shuts down the `_executor` once it is safe to do so.
    ss::future<> stop();

    // Returns `true` iff the provided `ntp` is managed by this scheduler.
    bool is_managed(const model::ntp&) const;

    // Pushes a new `ntp` to be managed by this scheduler to the list of `ntp`s.
    // It is the caller's responsibility to ensure the partition is not already
    // managed by this scheduler.
    void manage_partition(const model::ntp&);

    // Removes the `ntp` from the list of managed partitions. No-ops if the
    // provided `ntp` is not managed by this scheduler. Because the `ntp` may be
    // undergoing an inflight compaction, this function will block until it is
    // complete (an early stop is requested by this function).
    ss::future<> unmanage_partition(const model::ntp&);

private:
    using logs_type_t = chunked_hash_set<
      log_compaction_meta_ptr,
      log_compaction_meta_hash,
      log_compaction_meta_eq>;
    using log_list_t
      = intrusive_list<log_compaction_meta, &log_compaction_meta::link>;

    ss::future<> scheduling_loop();
    ss::future<> schedule_some();

    std::unique_ptr<log_collector> _log_collector;
    std::unique_ptr<scheduling_policy> _scheduling_policy;
    compaction_executor _executor;

    config::binding<std::chrono::milliseconds> _compaction_interval;

    // This semaphore is used as a way to signal a change to
    // `log_compaction_interval_ms` during the `wait()` operation in the main
    // scheduling loop.
    ssx::semaphore _scheduling_loop_sem{
      0, "cloud_topics::compaction::scheduling_loop"};

    // Abort source held and passed to executor.
    ss::abort_source _as;

    // Main gate held and passed to executor.
    ss::gate _gate;

    // Set of logs this scheduler is responsible for issuing compaction jobs
    // for.
    logs_type_t _logs;

    // Intrusive list of logs this scheduler is responsible for issuing
    // compaction jobs for.
    log_list_t _logs_list;
};

std::unique_ptr<compaction_scheduler>
  make_default_compaction_scheduler(log_collector_cluster_state);

} // namespace cloud_topics::l1
