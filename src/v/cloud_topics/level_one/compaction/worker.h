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

#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/compaction/committer.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "cloud_topics/level_one/compaction/source.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "compaction/key_offset_map.h"

namespace cloud_topics::l1 {

// A per-shard worker that accepts compaction jobs and performs de-duplication
// using a `sink`, `source`, and `reducer`.
// Can be pre-empted to either cancel or stop a compaction job.
class compaction_worker {
public:
    // io, metastore, and committer are all passed to the compaction `source`
    // and `sink`.
    compaction_worker(io*, metastore*, compaction_committer*);

    // Requests a compaction of the provided CTP and its `compaction_offsets`
    // as obtained from the `metastore`.
    ss::future<> compact(
      model::ntp,
      model::topic_id_partition,
      metastore::compaction_offsets_response,
      ss::abort_source&);

    // Sets `_state = compaction_job_state::hard_stop`, indicating the inflight
    // compaction job should stop promptly and abandon any in progress work,
    // e.g. during shutdown. It is up to users/currently running compaction jobs
    // to respect this flag. The worker will continue to accept compaction jobs
    // after this function is called.
    void request_hard_stop();

    // Sets `_state = compaction_job_state::soft_stop`. This is a request to
    // checkpoint any valuable progress from the inflight compaction job and
    // finish at earliest convenience, e.g. when a worker shard is being
    // pre-empted for various reasons. It is up to users/currently running
    // compaction jobs to respect this flag. The worker will continue to accept
    // compaction jobs after this function is called.
    void request_soft_stop();

    // Sets `_stopped` flag to indicate the `worker` will not perform any more
    // compaction jobs, as well as `_state = compaction_job_state::stopped` to
    // indicate to a potential inflight compaction job that it should exit
    // early. This should only be invoked during application shutdown.
    void stop_worker();

private:
    // Performs lazy initialization of the `compaction::key_offset_map` using
    // its reserved memory, if it is uninitialized.
    ss::future<> initialize_map();

private:
    // The state of the worker (`idle`, `running`, `cancelled`, or `stopped`).
    // `idle` means no compaction job is currently running on this worker.
    // `running` means a compaction job is inflight. `cancelled` means that the
    // inflight compaction job on this worker has been requested to checkpoint
    // its valuable progress and finish at earliest convenience (a graceful
    // stop), whereas `stopped` means that the inflight compaction job running
    // on this worker has been pre-empted to abandon all work and return as soon
    // as possible. `cancelled`/`stopped` do not mean that the worker itself is
    // stopped from running future compaction jobs (`_stopped` is used as a flag
    // to indicate this state instead).
    compaction_job_state _state{compaction_job_state::idle};

    std::unique_ptr<compaction::key_offset_map> _map{nullptr};

    // If `true`, new compaction jobs are automatically rejected (shutdown has
    // likely been requested).
    bool _stopped{false};

    // Owned by `app`.
    io* _io;

    // TODO: Owned by `app`.
    metastore* _metastore;

    // Owned by `scheduler`.
    compaction_committer* _committer;
};

} // namespace cloud_topics::l1
