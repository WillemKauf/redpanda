/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "base/outcome.h"
#include "base/seastarx.h"
#include "cluster/simple_batch_builder.h"
#include "container/chunked_vector.h"
#include "kafka/server/group_metadata.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include <optional>

namespace cluster {
class partition;
}

namespace kafka {

/**
 * Coalesces concurrent offset commit records for a single group coordinator
 * (__consumer_offsets) partition into shared raft batches.
 *
 * Each coordinator partition is served by exactly one shard, and clusters
 * with high commit rates saturate that shard long before the rest of the
 * node. Replicating every offset commit request as its own record batch
 * multiplies that load: each batch pays its own header, checksums and
 * per-batch storage, compaction and recovery costs, on a partition that
 * accumulates billions of records.
 *
 * While up to max_in_flight replications are outstanding, additional
 * commits accumulate into a single pending record batch that is flushed as
 * one raft batch as soon as an in-flight replication completes. Under low
 * concurrency every commit replicates immediately, so commit latency is
 * unchanged; coalescing only engages once the partition has more concurrent
 * commits than the replication pipeline can absorb - exactly the regime in
 * which the coordinator shard needs relief.
 *
 * Callers receive the log offset of the last record of their own commit
 * (computed from the merged batch's base offset), so log offset ordering
 * between commits - including commits for the same topic partition that
 * land in one merged batch - is identical to replicating them separately.
 */
class offset_commit_batcher
  : public ss::enable_lw_shared_from_this<offset_commit_batcher> {
public:
    /// records that exceed this count in the pending batch cause new
    /// commits to bypass coalescing and replicate directly, falling back
    /// to raft's own memory-based backpressure
    static constexpr size_t max_pending_records = 1024;
    /// replications kept in flight before commits start to coalesce
    static constexpr size_t max_in_flight = 2;

    struct stages {
        /// resolves when the records are accepted for replication
        ss::future<> dispatched;
        /// resolves with the log offset of the last record of this commit
        ss::future<result<model::offset>> committed;
    };

    explicit offset_commit_batcher(ss::lw_shared_ptr<cluster::partition> p);

    offset_commit_batcher(const offset_commit_batcher&) = delete;
    offset_commit_batcher& operator=(const offset_commit_batcher&) = delete;
    offset_commit_batcher(offset_commit_batcher&&) = delete;
    offset_commit_batcher& operator=(offset_commit_batcher&&) = delete;
    ~offset_commit_batcher();

    /// Replicates the given offset commit records to the coordinator
    /// partition with quorum ack in the given term, possibly merged into
    /// one record batch with concurrent commits of the same term.
    stages replicate(
      model::term_id term,
      chunked_vector<group_metadata_serializer::key_value> records);

private:
    struct waiter {
        /// number of records in the pending batch up to and including this
        /// commit's records
        size_t record_end;
        ss::promise<result<model::offset>> committed;
    };

    struct pending_batch {
        explicit pending_batch(model::term_id term)
          : term(term) {}

        model::term_id term;
        cluster::simple_batch_builder builder{
          model::record_batch_type::raft_data, model::offset(0)};
        size_t records{0};
        chunked_vector<std::unique_ptr<waiter>> waiters;
    };

    /// replicate a single commit's records as their own batch
    stages replicate_directly(
      model::term_id term,
      chunked_vector<group_metadata_serializer::key_value> records);

    /// flush the pending batch if an in-flight slot is available
    void maybe_flush();

    ss::future<result<model::offset>>
    do_replicate(model::term_id term, model::record_batch batch);

    ss::lw_shared_ptr<cluster::partition> _partition;
    std::optional<pending_batch> _pending;
    size_t _in_flight{0};
};

} // namespace kafka
