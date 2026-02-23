// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/consumer_group.h"

#include "base/vassert.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "model/namespace.h"

#include <seastar/core/sstring.hh>

#include <fmt/format.h>

#include <chrono>

namespace kafka {

consumer_group::~consumer_group() noexcept = default;

consumer_group::consumer_group(
  kafka::group_id id,
  config::configuration& conf,
  ss::lw_shared_ptr<cluster::partition> partition,
  model::term_id term,
  cluster::metadata_cache& metadata_cache)
  : _id(std::move(id))
  , _conf(conf)
  , _partition(std::move(partition))
  , _term(term)
  , _metadata_cache(metadata_cache)
  , _ctxlog(*this) {}

kafka::member_id consumer_group::generate_member_id() const {
    return kafka::member_id(fmt::format("{}-{}", _id(), uuid_t::create()));
}

void consumer_group::set_state(consumer_group_state s) {
    _ctxlog.debug(
      "Transitioning state: {} -> {}",
      consumer_group_state_to_string(_state),
      consumer_group_state_to_string(s));
    _state = s;
}

void consumer_group::bump_group_epoch() {
    _group_epoch = kafka::consumer_group_epoch(_group_epoch() + 1);
    _target_assignment_epoch = _group_epoch;
    _ctxlog.debug("Bumped group epoch to {}", _group_epoch);
    run_assignor();
}

void consumer_group::run_assignor() {
    // Build the list of assignable topics from subscribed topic names.
    std::vector<assignable_topic> topics;
    for (const auto& topic_name : _subscribed_topics) {
        auto tp_ns = model::topic_namespace(
          model::kafka_namespace, model::topic(topic_name));
        auto cfg = _metadata_cache.get_topic_cfg(tp_ns);
        if (!cfg) {
            _ctxlog.warn("Subscribed topic {} not found in metadata", topic_name);
            continue;
        }
        if (!cfg->tp_id) {
            _ctxlog.warn("Topic {} has no topic ID", topic_name);
            continue;
        }
        topics.push_back(assignable_topic{
          .id = *cfg->tp_id,
          .name = topic_name,
          .partition_count = cfg->partition_count,
        });
    }

    if (topics.empty()) {
        _ctxlog.debug("No assignable topics, clearing assignments");
        for (auto& [_, member] : _members) {
            member->set_target_assignment({});
        }
        return;
    }

    // Run the assignor.
    auto result = _assignor.assign(topics, _members);

    // Apply target assignments to members.
    for (auto& [mid, assignment] : result) {
        auto it = _members.find(mid);
        if (it != _members.end()) {
            it->second->set_target_assignment(std::move(assignment));
        }
    }

    _ctxlog.debug(
      "Assignor computed target assignments for {} members over {} topics",
      result.size(),
      topics.size());
}

bool consumer_group::all_members_at_target() const {
    for (const auto& [_, member] : _members) {
        if (!member->is_at_target()) {
            return false;
        }
    }
    return true;
}

void consumer_group::maybe_update_state() {
    if (_members.empty()) {
        set_state(consumer_group_state::empty);
        return;
    }
    if (all_members_at_target()) {
        set_state(consumer_group_state::stable);
    } else {
        set_state(consumer_group_state::reconciling);
    }
}

void consumer_group::remove_member(const kafka::member_id& id) {
    auto it = _members.find(id);
    if (it == _members.end()) {
        return;
    }

    auto& member = it->second;
    member->expire_timer().cancel();

    // Remove static member mapping if applicable
    if (member->instance_id()) {
        _static_members.erase(*member->instance_id());
    }

    _members.erase(it);
    _ctxlog.info("Removed member {}", id);

    // Rebuild subscribed topics
    _subscribed_topics.clear();
    for (const auto& [_, m] : _members) {
        for (const auto& topic : m->subscribed_topic_names()) {
            _subscribed_topics.insert(topic);
        }
    }

    bump_group_epoch();
    maybe_update_state();
}

void consumer_group::schedule_heartbeat_expiration(
  consumer_group_member_ptr member) {
    auto timeout = std::chrono::milliseconds(
      _conf.consumer_group_session_timeout_ms());

    member->expire_timer().cancel();
    member->expire_timer().set_callback(
      [this, mid = member->id()]() { remove_member(mid); });
    member->expire_timer().arm(timeout);
}

std::optional<
  chunked_vector<consumer_group_heartbeat_response_topic_partitions>>
consumer_group::build_response_assignment(
  const consumer_group_member_ptr& member) const {
    const auto& target = member->target_assignment();
    if (target.empty()) {
        return std::nullopt;
    }

    chunked_vector<consumer_group_heartbeat_response_topic_partitions> result;
    result.reserve(target.size());
    for (const auto& [topic_id, partitions] : target) {
        consumer_group_heartbeat_response_topic_partitions tp;
        tp.topic_id = topic_id;
        tp.partitions.reserve(partitions.size());
        for (const auto& p : partitions) {
            tp.partitions.push_back(p);
        }
        result.push_back(std::move(tp));
    }
    return result;
}

consumer_group_heartbeat_response
consumer_group::handle_join(const consumer_group_heartbeat_request_data& req) {
    // Check group max size
    if (
      static_cast<int32_t>(_members.size())
      >= _conf.consumer_group_max_size()) {
        return consumer_group_heartbeat_response(
          error_code::group_max_size_reached);
    }

    // Generate member ID
    auto new_member_id = generate_member_id();

    // Create member
    auto member = ss::make_lw_shared<consumer_group_member>(
      new_member_id,
      req.instance_id
        ? std::make_optional(kafka::group_instance_id(*req.instance_id))
        : std::nullopt,
      req.rack_id ? std::make_optional(*req.rack_id) : std::nullopt,
      req.rebalance_timeout_ms > std::chrono::milliseconds(0)
        ? req.rebalance_timeout_ms
        : _conf.consumer_group_session_timeout_ms(),
      req.subscribed_topic_names
        ? chunked_vector<ss::sstring>(req.subscribed_topic_names->begin(), req.subscribed_topic_names->end())
        : chunked_vector<ss::sstring>{},
      req.subscribed_topic_regex,
      req.server_assignor
        ? std::make_optional(kafka::server_assignor(*req.server_assignor))
        : std::nullopt);

    // Track static member
    if (member->instance_id()) {
        _static_members[*member->instance_id()] = new_member_id;
    }

    // Update subscribed topics
    for (const auto& topic : member->subscribed_topic_names()) {
        _subscribed_topics.insert(topic);
    }

    // Set initial epoch
    member->set_member_epoch(kafka::consumer_group_member_epoch(1));

    // Store member
    _members[new_member_id] = member;

    _ctxlog.info("New member {} joined", new_member_id);

    // Bump epoch and trigger assignment
    bump_group_epoch();
    set_state(consumer_group_state::assigning);

    // Schedule heartbeat expiration
    schedule_heartbeat_expiration(member);

    // Build response
    consumer_group_heartbeat_response resp;
    resp.data.error_code = error_code::none;
    resp.data.member_id = new_member_id;
    resp.data.member_epoch = member->member_epoch();
    resp.data.heartbeat_interval_ms
      = static_cast<int32_t>(_conf.consumer_group_heartbeat_interval_ms().count());
    resp.data.assignment = build_response_assignment(member);
    return resp;
}

consumer_group_heartbeat_response consumer_group::handle_leave(
  const consumer_group_heartbeat_request_data& req) {
    auto member_id = kafka::member_id(req.member_id);
    auto it = _members.find(member_id);
    if (it == _members.end()) {
        return consumer_group_heartbeat_response(error_code::unknown_member_id);
    }

    _ctxlog.info("Member {} leaving", member_id);
    remove_member(member_id);

    consumer_group_heartbeat_response resp;
    resp.data.error_code = error_code::none;
    resp.data.member_id = kafka::member_id(req.member_id);
    resp.data.member_epoch = kafka::consumer_group_member_epoch(-1);
    resp.data.heartbeat_interval_ms = 0;
    return resp;
}

consumer_group_heartbeat_response consumer_group::handle_heartbeat(
  const consumer_group_heartbeat_request_data& req) {
    auto member_id = kafka::member_id(req.member_id);
    auto it = _members.find(member_id);
    if (it == _members.end()) {
        return consumer_group_heartbeat_response(error_code::unknown_member_id);
    }

    auto& member = it->second;

    // Validate epoch
    if (req.member_epoch != member->member_epoch()) {
        _ctxlog.warn(
          "Member {} epoch mismatch: got {}, expected {}",
          member_id,
          req.member_epoch,
          member->member_epoch());
        return consumer_group_heartbeat_response(
          error_code::fenced_member_epoch);
    }

    // Update heartbeat timestamp
    member->set_latest_heartbeat(clock_type::now());
    schedule_heartbeat_expiration(member);

    // Update subscription if provided
    bool subscription_changed = false;
    if (req.subscribed_topic_names) {
        const auto& new_topics = *req.subscribed_topic_names;
        if (new_topics != member->subscribed_topic_names()) {
            chunked_vector<ss::sstring> copy;
            copy.reserve(new_topics.size());
            for (const auto& t : new_topics) {
                copy.push_back(t);
            }
            member->set_subscribed_topic_names(std::move(copy));
            subscription_changed = true;
        }
    }
    if (req.subscribed_topic_regex) {
        if (req.subscribed_topic_regex != member->subscribed_topic_regex()) {
            member->set_subscribed_topic_regex(req.subscribed_topic_regex);
            subscription_changed = true;
        }
    }

    // Update current assignment from member's report
    if (req.topic_partitions) {
        consumer_group_member::assignment_type current;
        for (const auto& tp : *req.topic_partitions) {
            std::vector<model::partition_id> parts;
            parts.reserve(tp.partitions.size());
            for (const auto& p : tp.partitions) {
                parts.push_back(p);
            }
            current[tp.topic_id] = std::move(parts);
        }
        member->set_current_assignment(std::move(current));
    }

    if (subscription_changed) {
        // Rebuild subscribed topics aggregate
        _subscribed_topics.clear();
        for (const auto& [_, m] : _members) {
            for (const auto& topic : m->subscribed_topic_names()) {
                _subscribed_topics.insert(topic);
            }
        }

        bump_group_epoch();
    }

    maybe_update_state();

    // Build response
    consumer_group_heartbeat_response resp;
    resp.data.error_code = error_code::none;
    resp.data.member_epoch = member->member_epoch();
    resp.data.heartbeat_interval_ms
      = static_cast<int32_t>(_conf.consumer_group_heartbeat_interval_ms().count());
    resp.data.assignment = build_response_assignment(member);
    return resp;
}

ss::future<consumer_group_heartbeat_response>
consumer_group::handle_consumer_group_heartbeat(
  consumer_group_heartbeat_request req) {
    const auto& data = req.data;

    if (_state == consumer_group_state::dead) {
        co_return consumer_group_heartbeat_response(
          error_code::coordinator_not_available);
    }

    // Dispatch based on member_epoch
    if (data.member_epoch == kafka::consumer_group_member_epoch(0)) {
        // Join
        co_return handle_join(data);
    } else if (data.member_epoch == kafka::consumer_group_member_epoch(-1)) {
        // Leave
        co_return handle_leave(data);
    } else {
        // Regular heartbeat
        co_return handle_heartbeat(data);
    }
}

} // namespace kafka
