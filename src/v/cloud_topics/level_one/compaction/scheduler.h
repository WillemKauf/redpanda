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
      std::unique_ptr<log_collector>, std::unique_ptr<scheduling_policy>);
    // Stops backgounded scheduling loop.
    ss::future<> stop();

    bool is_managed(const model::ntp&) const;
    void manage_partition(const model::ntp&);
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

} // namespace cloud_topics::l1
