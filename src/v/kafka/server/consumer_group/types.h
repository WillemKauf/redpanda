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

#include "base/format_to.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "kafka/protocol/types.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "serde/rw/chrono.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"
#include "serde/rw/optional.h"
#include "serde/rw/vector.h"
#include "utils/named_type.h"

#include "absl/container/node_hash_map.h"

#include <vector>

namespace kafka::consumer_group {

/// Member epoch: tracks the assignment epoch acknowledged by a member.
using member_epoch = named_type<int32_t, struct member_epoch_tag>;

/// Group epoch: monotonically increasing version of group state.
using group_epoch = named_type<int32_t, struct group_epoch_tag>;

/// Assignment epoch: version of the target assignment.
using assignment_epoch = named_type<int32_t, struct assignment_epoch_tag>;

/// The state of the consumer group.
enum class group_state : int8_t {
    empty = 0,
    assigning = 1,
    reconciling = 2,
    stable = 3,
    dead = 4,
};

std::ostream& operator<<(std::ostream& os, group_state s);

/// The reconciliation state of an individual member.
enum class member_assignment_state : int8_t {
    stable = 0,
    revoking = 1,
    assigning = 2,
};

std::ostream& operator<<(std::ostream& os, member_assignment_state s);

/// A set of partitions for a single topic.
struct topic_partitions
  : serde::envelope<
      topic_partitions,
      serde::version<0>,
      serde::compat_version<0>> {
    model::topic_id topic_id;
    chunked_vector<model::partition_id> partitions;

    auto serde_fields() { return std::tie(topic_id, partitions); }

    topic_partitions copy() const {
        return topic_partitions{
          .topic_id = topic_id, .partitions = partitions.copy()};
    }

    friend bool operator==(const topic_partitions&, const topic_partitions&)
      = default;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it,
          "{{topic_id: {}, num_partitions: {}}}",
          topic_id,
          partitions.size());
    }
};

/// Topic metadata tracked by the group.
struct topic_metadata
  : serde::envelope<
      topic_metadata,
      serde::version<0>,
      serde::compat_version<0>> {
    model::topic_id topic_id;
    model::topic topic_name;
    int32_t num_partitions{0};

    auto serde_fields() {
        return std::tie(topic_id, topic_name, num_partitions);
    }

    friend bool
    operator==(const topic_metadata&, const topic_metadata&) = default;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it,
          "{{topic_id: {}, topic_name: {}, num_partitions: {}}}",
          topic_id,
          topic_name(),
          num_partitions);
    }
};

/// Per-member state in a consumer group.
struct member
  : serde::envelope<member, serde::version<0>, serde::compat_version<0>> {
    kafka::member_id member_id;
    std::optional<kafka::group_instance_id> instance_id;
    std::optional<ss::sstring> rack_id;
    kafka::client_id client_id;
    kafka::client_host client_host;

    /// Topics the member subscribes to.
    chunked_vector<model::topic_id> subscribed_topic_ids;

    /// Server-side assignor name.
    ss::sstring server_assignor;

    /// Epoch tracking.
    member_epoch current_member_epoch{0};
    member_epoch previous_member_epoch{0};

    /// Reconciliation state.
    member_assignment_state assignment_state{member_assignment_state::stable};

    /// Partitions currently assigned and acknowledged.
    chunked_vector<topic_partitions> assigned_partitions;

    /// Partitions being revoked (member must release before progressing).
    chunked_vector<topic_partitions> revoking_partitions;

    /// Partitions pending assignment (waiting for revocations to complete).
    chunked_vector<topic_partitions> pending_partitions;

    /// Session and rebalance timeouts.
    std::chrono::milliseconds session_timeout{45000};
    std::chrono::milliseconds rebalance_timeout{300000};

    /// Last heartbeat time (not serialized, runtime only).
    model::timestamp last_heartbeat{model::timestamp::now()};

    auto serde_fields() {
        return std::tie(
          member_id,
          instance_id,
          rack_id,
          client_id,
          client_host,
          subscribed_topic_ids,
          server_assignor,
          current_member_epoch,
          previous_member_epoch,
          assignment_state,
          assigned_partitions,
          revoking_partitions,
          pending_partitions,
          session_timeout,
          rebalance_timeout);
    }

    member copy() const {
        auto copy_tps = [](const chunked_vector<topic_partitions>& src) {
            chunked_vector<topic_partitions> dst;
            dst.reserve(src.size());
            for (const auto& tp : src) {
                dst.push_back(tp.copy());
            }
            return dst;
        };
        member c;
        c.member_id = member_id;
        c.instance_id = instance_id;
        c.rack_id = rack_id;
        c.client_id = client_id;
        c.client_host = client_host;
        c.subscribed_topic_ids = subscribed_topic_ids.copy();
        c.server_assignor = server_assignor;
        c.current_member_epoch = current_member_epoch;
        c.previous_member_epoch = previous_member_epoch;
        c.assignment_state = assignment_state;
        c.assigned_partitions = copy_tps(assigned_partitions);
        c.revoking_partitions = copy_tps(revoking_partitions);
        c.pending_partitions = copy_tps(pending_partitions);
        c.session_timeout = session_timeout;
        c.rebalance_timeout = rebalance_timeout;
        c.last_heartbeat = last_heartbeat;
        return c;
    }

    friend bool operator==(const member&, const member&) = default;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it,
          "{{member_id: {}, epoch: {}, assignment_state: {}}}",
          member_id,
          current_member_epoch(),
          static_cast<int>(assignment_state));
    }
};

/// Per-member target assignment computed by the assignor.
struct target_assignment_member
  : serde::envelope<
      target_assignment_member,
      serde::version<0>,
      serde::compat_version<0>> {
    kafka::member_id member_id;
    chunked_vector<topic_partitions> partitions;

    auto serde_fields() { return std::tie(member_id, partitions); }

    target_assignment_member copy() const {
        chunked_vector<topic_partitions> p;
        p.reserve(partitions.size());
        for (const auto& tp : partitions) {
            p.push_back(tp.copy());
        }
        return target_assignment_member{
          .member_id = member_id, .partitions = std::move(p)};
    }

    friend bool
    operator==(const target_assignment_member&, const target_assignment_member&)
      = default;
};

/// The target assignment for the entire group.
struct target_assignment
  : serde::envelope<
      target_assignment,
      serde::version<0>,
      serde::compat_version<0>> {
    assignment_epoch epoch{0};
    chunked_vector<target_assignment_member> members;

    target_assignment copy() const {
        chunked_vector<target_assignment_member> m;
        m.reserve(members.size());
        for (const auto& mem : members) {
            m.push_back(mem.copy());
        }
        return target_assignment{.epoch = epoch, .members = std::move(m)};
    }

    auto serde_fields() { return std::tie(epoch, members); }

    friend bool
    operator==(const target_assignment&, const target_assignment&) = default;
};

/// A committed offset for a single topic-partition.
struct committed_offset
  : serde::envelope<
      committed_offset,
      serde::version<0>,
      serde::compat_version<0>> {
    model::topic topic;
    model::partition_id partition;
    kafka::offset offset;
    kafka::leader_epoch leader_epoch{kafka::invalid_leader_epoch};
    std::optional<ss::sstring> metadata;
    model::timestamp commit_timestamp;

    auto serde_fields() {
        return std::tie(
          topic, partition, offset, leader_epoch, metadata, commit_timestamp);
    }

    friend bool
    operator==(const committed_offset&, const committed_offset&) = default;
};

/// Key for offset lookups.
struct offset_key {
    model::topic topic;
    model::partition_id partition;

    friend bool operator==(const offset_key&, const offset_key&) = default;

    template<typename H>
    friend H AbslHashValue(H h, const offset_key& k) {
        return H::combine(std::move(h), k.topic, k.partition);
    }
};

/// The full in-memory state of a consumer group.
struct consumer_group_data {
    kafka::group_id group_id;
    group_epoch epoch{0};
    group_state state{group_state::empty};
    ss::sstring assignor_name;

    /// Members indexed by member_id.
    absl::node_hash_map<kafka::member_id, member> members;

    /// Topic metadata tracked by the group.
    chunked_vector<topic_metadata> topics;

    /// The current target assignment.
    target_assignment assignment;

    /// Committed offsets.
    absl::node_hash_map<offset_key, committed_offset> offsets;
};

} // namespace kafka::consumer_group
