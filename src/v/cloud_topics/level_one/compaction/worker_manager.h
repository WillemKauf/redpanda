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
#include "cloud_topics/level_one/compaction/committer.h"
#include "cloud_topics/level_one/compaction/logger.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "cloud_topics/level_one/compaction/worker.h"
#include "cloud_topics/level_one/metastore/replicated_metastore.h"
#include "container/chunked_hash_map.h"
#include "model/fundamental.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/sharded.hh>

class WorkerManagerTestFixture;

namespace cloud_topics::l1 {

// A worker_manager which exists as a singleton on shard0, owns a sharded pool
// of `compaction_worker`s, and provides access to a priority queue of CTPs
// which require compaction. Manages inflight compactions and can request early
// abort of inflight jobs.
// TODO: Hook this up to the AdminAPI to allow for users to customize which
// shards have active `compaction_worker`s, and persist that information in e.g.
// the kvstore.
class worker_manager {
public:
    static constexpr ss::shard_id worker_manager_shard = 0;

    worker_manager(
      pq_t&,
      ss::sharded<file_io>*,
      ss::sharded<replicated_metastore>*,
      ss::sharded<compaction_committer>*);

    // Starts the pool of workers, making them available for compaction jobs.
    ss::future<> start();

    // Destructs workers.
    // Should only be called after all inflight compactions have been stopped
    // (`request_stop_workers()` is a _request_ to stop inflight
    // compactions, but does not upon return guarantee all inflight jobs have
    // yet been stopped). During shutdown, the ideal function call order is:
    // ```cpp
    // // Request to stop inflight compactions and execution waiters.
    // auto stop_workers_fut = _worker_manager.request_stop_workers();
    // // Close all gates for all logs for potential inflight compactions.
    // co_await ss::max_concurrent_for_each(_logs.begin(),
    //                                      _logs.end(),
    //                                      max_concurrent_close,
    //                                      [](auto& log)
    //                                      { return log->gate.close(); });
    // // Finalize future
    // co_await std::move(stop_workers_fut);
    // // It is only safe to stop the worker_manager and destruct workers once
    // all
    // // gates have been closed.
    // co_await _worker_manager.stop();
    // ```
    ss::future<> stop();

    // Returns the top entry of `_work_queue`, if it is not empty, and sets
    // inflight state for the provided shard & CTP. Returns `std::nullopt` if
    // the `_work_queue` is empty.
    std::optional<log_compaction_meta*> try_acquire_work(ss::shard_id);

    // Erases inflight state.
    void finish_work(log_compaction_meta*, ss::shard_id);

    // If an inflight compaction job for the provided `tidp` exists, a signal is
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
    ss::future<> request_stop_compaction(model::topic_id_partition);

    // Alert all workers that new jobs have become available in the
    // `_work_queue`.
    ss::future<> alert_workers();

    // Pauses the worker on the provided shard.
    ss::future<> pause_worker(ss::shard_id);

    // Resumes the worker on the provided shard.
    ss::future<> resume_worker(ss::shard_id);

private:
    friend class ::WorkerManagerTestFixture;

    // Owned by `scheduler`.
    pq_t& _work_queue;

    // Owned by `app`.
    ss::sharded<file_io>* _io;

    // Owned by `app`.
    ss::sharded<replicated_metastore>* _metastore;

    // Owned by `scheduler`.
    ss::sharded<compaction_committer>* _committer;

    // Tracks inflight compaction jobs by mapping `tidp`s being compacted to the
    // `shard` on which they are being compacted.
    chunked_hash_map<model::topic_id_partition, ss::shard_id>
      _inflight_tidp_to_shard;

    // A sharded pool of compaction workers.
    ss::sharded<compaction_worker> _workers;
};

} // namespace cloud_topics::l1
