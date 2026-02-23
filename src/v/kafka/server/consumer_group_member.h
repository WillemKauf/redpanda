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
#include "container/chunked_vector.h"
#include "kafka/protocol/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/timer.hh>

#include <optional>

namespace kafka {

/// KIP-848: A member of a consumer group using the new consumer group protocol.
///
/// Key differences from the classic group_member:
/// - No join/sync promises (single-RPC protocol)
/// - Subscription stored as topic names + optional regex (not opaque bytes)
/// - Tracks member_epoch, target_assignment, and current_assignment
/// - Heartbeat timer for session expiry (same pattern as classic)
class consumer_group_member {
public:
    using clock_type = ss::lowres_clock;
    using assignment_type
      = chunked_hash_map<model::topic_id, std::vector<model::partition_id>>;

    consumer_group_member(
      kafka::member_id id,
      std::optional<kafka::group_instance_id> instance_id,
      std::optional<ss::sstring> rack_id,
      std::chrono::milliseconds rebalance_timeout,
      chunked_vector<ss::sstring> subscribed_topic_names,
      std::optional<ss::sstring> subscribed_topic_regex,
      std::optional<kafka::server_assignor> server_assignor)
      : _member_id(std::move(id))
      , _instance_id(std::move(instance_id))
      , _rack_id(std::move(rack_id))
      , _rebalance_timeout(rebalance_timeout)
      , _subscribed_topic_names(std::move(subscribed_topic_names))
      , _subscribed_topic_regex(std::move(subscribed_topic_regex))
      , _server_assignor(std::move(server_assignor))
      , _latest_heartbeat(clock_type::now()) {}

    const kafka::member_id& id() const { return _member_id; }
    const std::optional<kafka::group_instance_id>& instance_id() const {
        return _instance_id;
    }
    const std::optional<ss::sstring>& rack_id() const { return _rack_id; }
    std::chrono::milliseconds rebalance_timeout() const {
        return _rebalance_timeout;
    }

    /// The member epoch tracks progress through assignment epochs.
    kafka::consumer_group_member_epoch member_epoch() const {
        return _member_epoch;
    }
    void set_member_epoch(kafka::consumer_group_member_epoch epoch) {
        _member_epoch = epoch;
    }

    /// Subscription
    const chunked_vector<ss::sstring>& subscribed_topic_names() const {
        return _subscribed_topic_names;
    }
    void set_subscribed_topic_names(chunked_vector<ss::sstring> topics) {
        _subscribed_topic_names = std::move(topics);
    }
    const std::optional<ss::sstring>& subscribed_topic_regex() const {
        return _subscribed_topic_regex;
    }
    void set_subscribed_topic_regex(std::optional<ss::sstring> regex) {
        _subscribed_topic_regex = std::move(regex);
    }
    const std::optional<kafka::server_assignor>& server_assignor() const {
        return _server_assignor;
    }

    /// Target assignment is set by the group when computing new assignments.
    const assignment_type& target_assignment() const {
        return _target_assignment;
    }
    void set_target_assignment(assignment_type assignment) {
        _target_assignment = std::move(assignment);
    }

    /// Current assignment is reported by the member via heartbeat.
    const assignment_type& current_assignment() const {
        return _current_assignment;
    }
    void set_current_assignment(assignment_type assignment) {
        _current_assignment = std::move(assignment);
    }

    /// Returns true if the member has converged to its target assignment.
    bool is_at_target() const {
        return _current_assignment == _target_assignment;
    }

    /// Heartbeat timer for session expiry.
    ss::timer<clock_type>& expire_timer() { return _expire_timer; }

    void set_latest_heartbeat(clock_type::time_point tp) {
        _latest_heartbeat = tp;
    }
    clock_type::time_point latest_heartbeat() const {
        return _latest_heartbeat;
    }

    /// Check if the member should be considered alive given the current time
    /// and session timeout.
    bool should_keep_alive(
      clock_type::time_point deadline,
      std::chrono::milliseconds session_timeout) const {
        return _latest_heartbeat + session_timeout > deadline;
    }

private:
    kafka::member_id _member_id;
    std::optional<kafka::group_instance_id> _instance_id;
    std::optional<ss::sstring> _rack_id;
    std::chrono::milliseconds _rebalance_timeout;
    kafka::consumer_group_member_epoch _member_epoch{0};

    // Subscription
    chunked_vector<ss::sstring> _subscribed_topic_names;
    std::optional<ss::sstring> _subscribed_topic_regex;
    std::optional<kafka::server_assignor> _server_assignor;

    // Assignments
    assignment_type _target_assignment;
    assignment_type _current_assignment;

    // Heartbeat tracking
    clock_type::time_point _latest_heartbeat;
    ss::timer<clock_type> _expire_timer;
};

using consumer_group_member_ptr = ss::lw_shared_ptr<consumer_group_member>;

} // namespace kafka
