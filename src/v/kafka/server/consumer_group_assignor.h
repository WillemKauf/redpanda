/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "container/chunked_hash_map.h"
#include "kafka/protocol/types.h"
#include "kafka/server/consumer_group_member.h"
#include "model/fundamental.h"
#include "model/metadata.h"

#include "absl/container/node_hash_map.h"

#include <vector>

namespace kafka {

/// Metadata about a subscribable topic, used by assignors.
struct assignable_topic {
    model::topic_id id;
    ss::sstring name;
    int32_t partition_count;
};

/// Result of running an assignor: per-member target assignments.
using assignor_result
  = chunked_hash_map<kafka::member_id, consumer_group_member::assignment_type>;

/// Interface for server-side consumer group partition assignors (KIP-848).
///
/// Assignors take the set of subscribable topics, member subscriptions,
/// and current assignments, and produce new target assignments.
class consumer_group_assignor {
public:
    virtual ~consumer_group_assignor() = default;

    /// The name of this assignor (e.g., "uniform", "range").
    virtual const kafka::server_assignor& name() const = 0;

    /// Compute target assignments for all members.
    ///
    /// \param topics  The set of topics that are subscribed by at least one
    ///                member, with their partition counts.
    /// \param members The current group members with their subscriptions and
    ///                current assignments.
    /// \return A map from member_id to their new target assignment.
    virtual assignor_result assign(
      const std::vector<assignable_topic>& topics,
      const absl::node_hash_map<kafka::member_id, consumer_group_member_ptr>&
        members)
      = 0;
};

/// Uniform assignor: distributes partitions as evenly as possible across
/// members subscribed to each topic. This is the default assignor for
/// KIP-848 consumer groups.
///
/// Algorithm:
/// - For each topic, collect all members subscribed to it.
/// - Distribute partitions round-robin across those members.
/// - This ensures each member gets floor(N/M) or ceil(N/M) partitions
///   per topic, where N is partition count and M is subscriber count.
class uniform_assignor final : public consumer_group_assignor {
public:
    const kafka::server_assignor& name() const override;

    assignor_result assign(
      const std::vector<assignable_topic>& topics,
      const absl::node_hash_map<kafka::member_id, consumer_group_member_ptr>&
        members) override;

private:
    static const kafka::server_assignor _name;
};

} // namespace kafka
