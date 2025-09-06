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
#include "cloud_topics/level_one/compaction/meta.h"
#include "cloud_topics/level_one/compaction/worker.h"
#include "container/chunked_circular_buffer.h"
#include "container/chunked_hash_map.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "ssx/future-util.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/sharded.hh>

namespace cloud_topics::l1 {

class compaction_executor {
public:
    compaction_executor(ss::abort_source&, ss::gate&);

    // Starts the pool of workers, making them available for compaction jobs.
    ss::future<> start();

    // Breaks condition variable and destructs workers.
    // Should only be called after all inflight compactions have been stopped
    // (`request_stop_inflight_compactions()` is a _request_ to stop inflight
    // compactions, but does not upon return guarantee all inflight jobs have
    // yet been stopped)
    ss::future<> stop();

    // Waits for an available worker from the pool and then issues a
    // backgrounded compaction job for the provided `log` on that worker's
    // shard.
    ss::future<> compact_one(log_compaction_meta*);

    // If an inflight compaction job for the provided `ntp` exists, a signal is
    // sent to the shard on which the job is occurring to request an early
    // abort. The returned future from this function does not, upon resolving,
    // guarantee that the inflight compaction (if underway) has been stopped,
    // only that a request has been made to stop it promptly.
    ss::future<> request_stop_compaction(model::ntp ntp);

    // Requests that all inflight compaction jobs be stopped promptly. The
    // returned future from this function does not, upon resolving, guarantee
    // that the inflight compactions (if underway) have been stopped, only that
    // requests have been made to stop them promptly.
    ss::future<> request_stop_inflight_compactions();

private:
    using worker_shard = ss::shard_id;

    // Dispatches a background compaction job for the provided `log` on the
    // provided `worker_shard`.
    void do_compact(worker_shard, log_compaction_meta*);

    // Returns a shard for which a compaction job can be immediately scheduled
    // on the local worker. If no worker is immediatel available, one is waited
    // upon.
    ss::future<worker_shard> get_available_worker();

    // A reference to the owning `compaction_scheduler's` abort source.
    ss::abort_source& _as;

    // A reference to the owning `compaction_scheduler's` gate.
    ss::gate& _gate;

    // Used to alert worker waiters that a shard has become available.
    ss::condition_variable _cvar;

    // Tracks available workers and is used as a pool from which new compaction
    // jobs can be issued.
    chunked_circular_buffer<worker_shard> _avail_workers;

    // Tracks inflight compaction jobs by mapping `ntp`s being compacted to the
    // `shard` on which they are being compacted.
    chunked_hash_map<model::ntp, worker_shard> _inflight;

    // A sharded pool of compaction workers.
    ss::sharded<compaction_worker> _workers;
};

} // namespace cloud_topics::l1
