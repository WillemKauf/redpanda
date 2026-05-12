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

#include "cloud_topics/level_one/common/file_io.h"
#include "cloud_topics/level_one/maintenance/logger.h"
#include "cloud_topics/level_one/maintenance/meta.h"
#include "cloud_topics/level_one/maintenance/scheduler_probe.h"
#include "cloud_topics/level_one/maintenance/worker.h"
#include "cloud_topics/level_one/metastore/replicated_metastore.h"
#include "cluster/metadata_cache.h"
#include "container/chunked_hash_map.h"
#include "model/fundamental.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/sharded.hh>

class WorkerManagerTestFixture;
class SchedulerTestFixture;

namespace cloud_topics::l1 {

// A worker_manager which exists as a singleton on shard0, owns a sharded pool
// of `compaction_worker`s, and provides access to two priority queues of CTPs
// that require maintenance work: one for compaction and one for leveling.
// Manages inflight jobs and can request early abort of inflight jobs.
// TODO: Hook this up to the AdminAPI to allow for users to customize which
// shards have active `compaction_worker`s, and persist that information in e.g.
// the kvstore.
class worker_manager {
public:
    static constexpr ss::shard_id worker_manager_shard = 0;

    worker_manager(
      log_compaction_queue&,
      log_leveling_queue&,
      ss::sharded<file_io>*,
      ss::sharded<replicated_metastore>*,
      ss::sharded<cluster::metadata_cache>*,
      compaction_scheduler_probe&,
      ss::sharded<level_one_reader_probe>*);

    // Starts the pool of workers, making them available for maintenance jobs.
    ss::future<> start();

    // Stops all workers (and inflight jobs) and then destructs workers.
    // Workers will no longer accept jobs after this function has been called,
    // and waiters will be declined. This should only be invoked during
    // application shutdown.
    ss::future<> stop();

    // Returns the top entry of either the compaction or leveling queue, if
    // available, and sets inflight state for the provided shard & CTP. Returns
    // `std::nullopt` if both queues are empty. Compaction is preferred over
    // leveling when both queues have work, since compaction reduces data
    // volume and can change what's worth leveling.
    std::optional<std::pair<foreign_log_compaction_meta_ptr, job_kind>>
      try_acquire_work(ss::shard_id);

    // Resets inflight state for the provided CTP. `kind` indicates which
    // queue the work came from and which probe counter to bump.
    void complete_work(log_compaction_meta*, job_kind);

    // If an inflight compaction job for the provided log exists, a signal is
    // sent to the worker shard on which the job is occurring to request an
    // early abort. The returned future from this function does not, upon
    // resolving, guarantee that the inflight compaction (if underway) has been
    // stopped, only that a pre-emption request has been made.
    //
    // Note that stopping compaction is much different than fully stopping a
    // worker. This function leaves the worker in a valid state, allowing future
    // compaction jobs to be ran. This function is ideally used when e.g. a
    // partition is removed or the `cleanup.policy` for a topic is changed and a
    // single compaction job must be stopped.
    void request_stop_compaction(log_compaction_meta_ptr);

    // If an inflight leveling job is running on the provided shard, sends a
    // soft-stop signal so the job checkpoints and exits, allowing compaction to
    // take priority. Does not affect the worker itself.
    ss::future<> interrupt_leveling_job(ss::shard_id);

    // Alert all workers that new jobs have become available in the
    // `_work_queue`.
    ss::future<> alert_workers();

    // Pauses the worker on the provided shard.
    ss::future<> pause_worker(ss::shard_id);

    // Resumes the worker on the provided shard.
    ss::future<> resume_worker(ss::shard_id);

private:
    friend class ::WorkerManagerTestFixture;
    friend class ::SchedulerTestFixture;

    // Owned by `scheduler`.
    log_compaction_queue& _compaction_queue;

    // Owned by `scheduler`.
    log_leveling_queue& _leveling_queue;

    // Owned by `app`.
    ss::sharded<file_io>* _io;

    // Owned by `app`.
    ss::sharded<replicated_metastore>* _metastore;

    ss::sharded<cluster::metadata_cache>* _metadata_cache;

    // Owned by `scheduler`.
    compaction_scheduler_probe& _probe;

    // Owned by `app`.
    ss::sharded<level_one_reader_probe>* _l1_reader_probe;

    // A sharded pool of compaction workers.
    ss::sharded<compaction_worker> _workers;

    ss::gate _gate;
};

} // namespace cloud_topics::l1
