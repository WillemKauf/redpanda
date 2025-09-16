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
#include "container/chunked_circular_buffer.h"
#include "container/chunked_hash_map.h"
#include "container/intrusive_list_helpers.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "ssx/future-util.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/sharded.hh>

class ExecutorTestFixture;

namespace cloud_topics::l1 {

// An executor which exists as a singleton on shard0, owns a sharded pool of
// `compaction_worker`s, and dispatches compaction jobs to available shards.
// Manages inflight compactions and can request early abort of inflight jobs.
// TODO: Hook this up to the AdminAPI to allow for users to customize which
// shards have active `compaction_worker`s, and persist that information in e.g.
// the kvstore.
class compaction_executor {
public:
    using worker_shard = ss::shard_id;
    // Metadata for a worker managed by this executor.
    struct worker_meta {
        explicit worker_meta(worker_shard shard)
          : shard(shard) {}

        // Describes whether a worker on a given shard is `active` and available
        // for compaction jobs, or `paused` and unavailable.
        enum class state { active, paused };

        // The shard this worker lives on.
        worker_shard shard;

        // The state of the worker (see above enum).
        state current_state{state::active};

        // If set, this is the `topic_id_partition` which is currently
        // undergoing an inflight compaction job on this worker. If not set, the
        // worker is idle.
        std::optional<model::topic_id_partition> tidp{std::nullopt};

        intrusive_list_hook link;
    };

    compaction_executor(
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
    // auto stop_workers_fut = _executor.request_stop_workers();
    // // Close all gates for all logs for potential inflight compactions.
    // co_await ss::max_concurrent_for_each(_logs.begin(),
    //                                      _logs.end(),
    //                                      max_concurrent_close,
    //                                      [](auto& log)
    //                                      { return log->gate.close(); });
    // // Finalize future
    // co_await std::move(stop_workers_fut);
    // // It is only safe to stop the executor and destruct workers once all
    // // gates have been closed.
    // co_await _executor.stop();
    // ```
    ss::future<> stop();

    // Waits for an available worker from the pool and then issues a
    // compaction job for the provided `log` on that worker's shard.
    // This future resolves when the compaction job is finished.
    ss::future<> compact_log(log_info_and_meta);

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

    // Requests that all workers (and inflight compaction jobs) be stopped
    // promptly, and requests an abort of the local abort source. Workers will
    // no longer accept compaction jobs after this function has been called, and
    // waiters will be declined. The returned future from this function does
    // not, upon resolving, guarantee that inflight compactions (if any) have
    // been stopped, only that pre-emption requests have been made. This should
    // only be invoked during application shutdown; see comment above in
    // `stop()` for required ordering.
    ss::future<> request_stop_workers();

    // Sets the state of the worker on the provided shard to either `active` or
    // `paused`. Setting the state of a worker to its current state is a no-op.
    // * Setting the state of an `active` worker to `paused` requires checking
    // if the worker has a currently inflight compaction running.
    // If it does,
    //  - A cancellation request (graceful shutdown) is made to the compaction
    //  - job on that worker while preventing the worker from being returned to
    //  - the `_avail_workers` pool after its completion.
    // If it does not,
    //  - Simply remove the worker from the `_avail_workers` pool.
    // * Setting the state of a `paused` worker to `active` requires checking if
    // the worker has a currently inflight compaction running.
    // If it does (e.g someone is flipping the `paused`/`active` switch very
    // quickly before an inflight compaction has the chance to gracefully
    // finish),
    //  - The job is allowed to finish, and no further steps need to be
    //  - taken, as the worker will be returned to the `_avail_workers` pool
    //  - upon completion.
    // If it does not,
    //  - The worker is added back to the `_avail_workers` pool and a single
    //  - waiter is notified.
    ss::future<> set_worker_state(worker_shard, worker_meta::state);

private:
    // Populate the `_worker_meta` container and `_avail_workers` pool with
    // every shard available for compaction, and then awake any waiters that may
    // already exist.
    // TODO: kvstore state to persist paused/unpaused workers across restarts.
    void populate_workers();

    // Helper function to acquire metadata for a worker on a particular shard.
    worker_meta& get_worker_meta(worker_shard);

    // Dispatches a compaction job for the provided `log` on the
    // provided `worker_shard`.
    ss::future<> do_compact_log(worker_shard, log_info_and_meta);

    // Returns a shard for which a compaction job can be immediately scheduled
    // on the local worker. If no worker is immediatel available, one is waited
    // upon.
    ss::future<worker_shard>
    get_available_worker(const model::topic_id_partition&);

    // Erases the provided `tidp` from `_inflight_tidps` list, resets worker
    // `tidp` metadata and returns the worker back to the `_avail_workers` pool
    // (if it has not been put on pause in the interim). Meant to be called
    // after a worker has finished its inflight compaction job.
    void return_worker(worker_shard, const model::topic_id_partition&);

private:
    using worker_meta_ptr = std::unique_ptr<worker_meta>;
    struct worker_meta_ptr_hash {
        using is_transparent = void;
        size_t operator()(const worker_meta_ptr& w) const noexcept {
            return std::hash<worker_shard>{}(w->shard);
        }
        size_t operator()(const worker_shard& s) const noexcept {
            return std::hash<worker_shard>{}(s);
        }
    };
    struct worker_meta_ptr_eq {
        using is_transparent = void;
        bool operator()(const worker_meta_ptr& lhs, const worker_meta_ptr& rhs)
          const noexcept {
            return lhs->shard == rhs->shard;
        }
        bool operator()(
          const worker_meta_ptr& lhs, const worker_shard& rhs) const noexcept {
            return lhs->shard == rhs;
        }
        bool operator()(
          const worker_shard& lhs, const worker_meta_ptr& rhs) const noexcept {
            return lhs == rhs->shard;
        }
    };
    using worker_set = chunked_hash_set<
      worker_meta_ptr,
      worker_meta_ptr_hash,
      worker_meta_ptr_eq>;
    using worker_list = intrusive_list<worker_meta, &worker_meta::link>;

private:

    // Owned by `app`.
    ss::sharded<file_io>* _io;

    // TODO: Owned by `app`.
    ss::sharded<replicated_metastore>* _metastore;

    // Owned by `scheduler`.
    ss::sharded<compaction_committer>* _committer;

    ss::abort_source _as;

    // Used to alert waiters that a worker has become available.
    ss::condition_variable _cvar;

    // Tracks available workers and is used as a pool from which new
    // compaction jobs can be issued.
    worker_list _avail_workers;

    // Tracks worker metadata, such as the `shard` on which this `worker` lives,
    // the `current_state` (which can either be `active` or `paused`), and the
    // `tidp` (which can be set or unset, depending on if this worker has a
    // compaction inflight).
    worker_set _worker_meta;

    // Tracks inflight compaction jobs by mapping `tidp`s being compacted to the
    // `shard` on which they are being compacted.
    chunked_hash_map<model::topic_id_partition, worker_shard>
      _inflight_tidp_to_shard;

    // A sharded pool of compaction workers.
    ss::sharded<compaction_worker> _workers;
};

} // namespace cloud_topics::l1
