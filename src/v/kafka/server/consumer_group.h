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

#include "base/seastarx.h"
#include "cluster/fwd.h"
#include "config/configuration.h"
#include "container/chunked_hash_map.h"
#include "kafka/protocol/consumer_group_heartbeat.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/offset_commit.h"
#include "kafka/protocol/offset_fetch.h"
#include "kafka/protocol/types.h"
#include "kafka/server/consumer_group_assignor.h"
#include "kafka/server/group_metadata.h"
#include "kafka/server/consumer_group_member.h"
#include "kafka/server/consumer_group_state.h"
#include "kafka/server/logger.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>

#include "absl/container/node_hash_map.h"
#include "absl/container/node_hash_set.h"

namespace kafka {

/// KIP-848: A consumer group using the new consumer group protocol.
///
/// Parallel to the classic `group` class. Key differences:
/// - Server-side assignment (no client leader)
/// - Epoch-based convergence (no global stop-the-world rebalance)
/// - Single RPC: ConsumerGroupHeartbeat replaces JoinGroup + SyncGroup +
///   Heartbeat
class consumer_group {
public:
    using clock_type = ss::lowres_clock;

    /// Committed offset metadata for a single topic-partition.
    struct offset_metadata {
        model::offset offset;
        kafka::leader_epoch committed_leader_epoch{-1};
        ss::sstring metadata;
        model::timestamp commit_timestamp;
    };

    consumer_group(
      kafka::group_id id,
      config::configuration& conf,
      ss::lw_shared_ptr<cluster::partition> partition,
      model::term_id term,
      cluster::metadata_cache& metadata_cache);

    ~consumer_group() noexcept;

    const kafka::group_id& id() const { return _id; }
    consumer_group_state state() const { return _state; }
    kafka::consumer_group_epoch group_epoch() const { return _group_epoch; }

    /// Process a ConsumerGroupHeartbeat request.
    ss::future<consumer_group_heartbeat_response>
    handle_consumer_group_heartbeat(consumer_group_heartbeat_request req);

    /// Handle an OffsetCommit request for this consumer group.
    offset_commit_response
    handle_offset_commit(const offset_commit_request& req);

    /// Handle an OffsetFetch request for this consumer group.
    offset_fetch_response handle_offset_fetch(
      const offset_fetch_request& req) const;

    /// Number of active members.
    size_t num_members() const { return _members.size(); }

    /// Check if the group has no members.
    bool is_empty() const { return _members.empty(); }

    /// Remove a member by ID (e.g., on session timeout or explicit leave).
    void remove_member(const kafka::member_id& id);

    /// Schedule heartbeat expiration timer for a member.
    void schedule_heartbeat_expiration(consumer_group_member_ptr member);

    /// Notify the group about topic metadata changes. If any subscribed
    /// topics were affected, bumps the group epoch and recomputes
    /// assignments.
    void notify_topic_metadata_changed(
      const absl::node_hash_set<model::topic>& changed_topics);

    /// Recover state from persisted metadata (on leadership change).
    void recover_from_metadata(consumer_group_metadata_value md);

    /// Build a metadata value from current in-memory state for persistence.
    consumer_group_metadata_value build_metadata_value() const;

    /// Persist current state to the __consumer_offsets partition via raft.
    ss::future<> checkpoint();

private:
    /// Generate a new unique member ID.
    kafka::member_id generate_member_id() const;

    /// Bump the group epoch and run the assignor.
    void bump_group_epoch();

    /// Run the assignor and apply target assignments to members.
    void run_assignor();

    /// Set the group state.
    void set_state(consumer_group_state s);

    /// Check if all members have converged to their target assignments.
    bool all_members_at_target() const;

    /// Update group state based on member convergence.
    void maybe_update_state();

    /// Handle a join (member_epoch == 0).
    consumer_group_heartbeat_response
    handle_join(const consumer_group_heartbeat_request_data& req);

    /// Handle a leave (member_epoch == -1).
    consumer_group_heartbeat_response
    handle_leave(const consumer_group_heartbeat_request_data& req);

    /// Handle a regular heartbeat (member_epoch > 0).
    consumer_group_heartbeat_response
    handle_heartbeat(const consumer_group_heartbeat_request_data& req);

    /// Build the response assignment from a member's target assignment.
    std::optional<
      chunked_vector<consumer_group_heartbeat_response_topic_partitions>>
    build_response_assignment(const consumer_group_member_ptr& member) const;

    /// Context-aware logging helper.
    class ctx_log {
    public:
        explicit ctx_log(const consumer_group& group)
          : _group(group) {}

        template<typename... Args>
        void info(const char* format, Args&&... args) const {
            log(ss::log_level::info, format, std::forward<Args>(args)...);
        }

        template<typename... Args>
        void warn(const char* format, Args&&... args) const {
            log(ss::log_level::warn, format, std::forward<Args>(args)...);
        }

        template<typename... Args>
        void debug(const char* format, Args&&... args) const {
            log(ss::log_level::debug, format, std::forward<Args>(args)...);
        }

        template<typename... Args>
        void trace(const char* format, Args&&... args) const {
            log(ss::log_level::trace, format, std::forward<Args>(args)...);
        }

    private:
        template<typename... Args>
        void log(
          ss::log_level lvl, const char* format, Args&&... args) const {
            if (klog.is_enabled(lvl)) {
                auto line = fmt::format(
                  "[N:{} S:{} E:{}] {}",
                  _group._id,
                  _group._state,
                  _group._group_epoch,
                  fmt::format(
                    fmt::runtime(format), std::forward<Args>(args)...));
                klog.log(lvl, "{}", line);
            }
        }

        const consumer_group& _group;
    };

    kafka::group_id _id;
    consumer_group_state _state{consumer_group_state::empty};
    kafka::consumer_group_epoch _group_epoch{0};
    kafka::consumer_group_epoch _target_assignment_epoch{0};
    config::configuration& _conf;
    ss::lw_shared_ptr<cluster::partition> _partition;
    model::term_id _term;
    cluster::metadata_cache& _metadata_cache;
    uniform_assignor _assignor;

    // Members
    using member_map
      = absl::node_hash_map<kafka::member_id, consumer_group_member_ptr>;
    member_map _members;

    // Static member mapping (instance_id → member_id)
    chunked_hash_map<kafka::group_instance_id, kafka::member_id>
      _static_members;

    // Aggregate subscribed topics across all members
    absl::node_hash_set<ss::sstring> _subscribed_topics;

    // Committed offsets
    chunked_hash_map<model::topic_partition, offset_metadata> _offsets;

    ctx_log _ctxlog;
};

} // namespace kafka
