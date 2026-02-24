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

#include "kafka/server/consumer_group/coordinator.h"

#include "kafka/server/consumer_group/assignor.h"
#include "kafka/server/consumer_group/commands.h"
#include "kafka/server/logger.h"
#include "model/record_batch_types.h"
#include "serde/rw/rw.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/coroutine.hh>

namespace kafka::consumer_group {

namespace {

/// Build a record batch from a single command.
template<typename Cmd>
model::record_batch make_batch(typename Cmd::value v) {
    storage::record_batch_builder builder(
      model::record_batch_type::consumer_group, model::offset{0});
    auto key_buf = serde::to_iobuf(Cmd::key);
    auto val_buf = serde::to_iobuf(std::move(v));
    builder.add_raw_kv(std::move(key_buf), std::move(val_buf));
    return std::move(builder).build();
}

constexpr auto replicate_timeout = std::chrono::seconds{5};

kafka::member_id generate_member_id() {
    return kafka::member_id{fmt::format("member-{}", uuid_t::create())};
}

kafka::consumer_group_heartbeat_response_data
make_error_response(kafka::error_code ec) {
    kafka::consumer_group_heartbeat_response_data resp;
    resp.error_code = ec;
    return resp;
}

} // namespace

coordinator::coordinator(
  consumer_group_stm& stm,
  std::chrono::milliseconds heartbeat_interval,
  std::chrono::milliseconds session_timeout)
  : _stm(stm)
  , _heartbeat_interval(heartbeat_interval)
  , _session_timeout(session_timeout)
  , _session_timer([this] { expire_sessions(); }) {}

coordinator::~coordinator() = default;

void coordinator::start() {
    _session_timer.arm_periodic(
      std::chrono::duration_cast<ss::lowres_clock::duration>(_session_timeout));
}

ss::future<> coordinator::stop() {
    _as.request_abort();
    _session_timer.cancel();
    co_await _gate.close();
}

ss::future<kafka::consumer_group_heartbeat_response_data>
coordinator::heartbeat(kafka::consumer_group_heartbeat_request_data req) {
    auto holder = _gate.hold();

    if (req.member_epoch == 0) {
        co_return co_await handle_join(std::move(req));
    } else if (req.member_epoch == -1) {
        co_return co_await handle_leave(std::move(req));
    } else {
        co_return co_await handle_heartbeat(std::move(req));
    }
}

ss::future<kafka::consumer_group_heartbeat_response_data>
coordinator::handle_join(kafka::consumer_group_heartbeat_request_data req) {
    const auto& gid = req.group_id;

    auto* group = _stm.find_group(gid);
    auto new_member_id = generate_member_id();

    // Build member state.
    member m;
    m.member_id = new_member_id;
    m.instance_id = req.instance_id;
    m.rack_id = req.rack_id;
    m.client_id = kafka::client_id{""};
    m.client_host = kafka::client_host{""};
    m.server_assignor = req.server_assignor.value_or("");
    m.session_timeout = _session_timeout;
    if (req.rebalance_timeout_ms >= std::chrono::milliseconds{0}) {
        m.rebalance_timeout = req.rebalance_timeout_ms;
    }
    m.last_heartbeat = model::timestamp::now();

    // Upsert the member.
    {
        upsert_member_cmd::value v;
        v.group_id = gid;
        v.member_data = std::move(m);
        auto ec = co_await _stm.replicate_and_wait(
          make_batch<upsert_member_cmd>(std::move(v)), replicate_timeout);
        if (ec) {
            co_return make_error_response(
              kafka::error_code::coordinator_not_available);
        }
    }

    // If the group is new or empty, update group metadata.
    if (!group || group->state == group_state::empty) {
        update_group_metadata_cmd::value v;
        v.group_id = gid;
        v.epoch = group_epoch{group ? group->epoch() + 1 : 1};
        v.state = group_state::stable;
        v.assignor_name = m.server_assignor;
        auto ec = co_await _stm.replicate_and_wait(
          make_batch<update_group_metadata_cmd>(std::move(v)),
          replicate_timeout);
        if (ec) {
            co_return make_error_response(
              kafka::error_code::coordinator_not_available);
        }
    }

    // Trigger assignment.
    auto ec = co_await run_assignor(gid);
    if (ec) {
        co_return make_error_response(
          kafka::error_code::coordinator_not_available);
    }

    // Build response.
    auto* updated_group = _stm.find_group(gid);
    if (!updated_group) {
        co_return make_error_response(kafka::error_code::group_id_not_found);
    }

    auto member_it = updated_group->members.find(new_member_id);
    if (member_it == updated_group->members.end()) {
        co_return make_error_response(kafka::error_code::unknown_member_id);
    }

    auto resp = build_response(gid, member_it->second);
    resp.member_id = new_member_id;
    co_return resp;
}

ss::future<kafka::consumer_group_heartbeat_response_data>
coordinator::handle_leave(kafka::consumer_group_heartbeat_request_data req) {
    const auto& gid = req.group_id;

    auto* group = _stm.find_group(gid);
    if (!group) {
        co_return make_error_response(kafka::error_code::group_id_not_found);
    }

    auto member_it = group->members.find(req.member_id);
    if (member_it == group->members.end()) {
        co_return make_error_response(kafka::error_code::unknown_member_id);
    }

    // Remove the member.
    {
        remove_member_cmd::value v;
        v.group_id = gid;
        v.member_id = req.member_id;
        auto ec = co_await _stm.replicate_and_wait(
          make_batch<remove_member_cmd>(std::move(v)), replicate_timeout);
        if (ec) {
            co_return make_error_response(
              kafka::error_code::coordinator_not_available);
        }
    }

    // If group is now empty, mark it as such. Otherwise reassign.
    auto* updated_group = _stm.find_group(gid);
    if (updated_group && updated_group->members.empty()) {
        update_group_metadata_cmd::value v;
        v.group_id = gid;
        v.epoch = group_epoch{updated_group->epoch() + 1};
        v.state = group_state::empty;
        v.assignor_name = updated_group->assignor_name;
        co_await _stm.replicate_and_wait(
          make_batch<update_group_metadata_cmd>(std::move(v)),
          replicate_timeout);
    } else if (updated_group && !updated_group->members.empty()) {
        co_await run_assignor(gid);
    }

    kafka::consumer_group_heartbeat_response_data resp;
    resp.member_epoch = -1;
    resp.heartbeat_interval_ms = static_cast<int32_t>(
      _heartbeat_interval.count());
    co_return resp;
}

ss::future<kafka::consumer_group_heartbeat_response_data>
coordinator::handle_heartbeat(
  kafka::consumer_group_heartbeat_request_data req) {
    const auto& gid = req.group_id;

    auto* group = _stm.find_group(gid);
    if (!group) {
        co_return make_error_response(kafka::error_code::group_id_not_found);
    }

    auto member_it = group->members.find(req.member_id);
    if (member_it == group->members.end()) {
        co_return make_error_response(kafka::error_code::unknown_member_id);
    }

    const auto& m = member_it->second;

    // Epoch check: fenced if the member's epoch doesn't match.
    if (req.member_epoch != m.current_member_epoch()) {
        co_return make_error_response(kafka::error_code::fenced_member_epoch);
    }

    // Update last heartbeat time (runtime only, not replicated).
    const_cast<member&>(m).last_heartbeat = model::timestamp::now();

    co_return build_response(gid, m);
}

ss::future<std::error_code>
coordinator::run_assignor(const kafka::group_id& gid) {
    auto* group = _stm.find_group(gid);
    if (!group) {
        co_return std::error_code{};
    }

    auto new_epoch = assignment_epoch{group->assignment.epoch() + 1};
    auto ta = compute_target_assignment(
      new_epoch, group->members, group->topics);

    set_target_assignment_cmd::value v;
    v.group_id = gid;
    v.assignment = std::move(ta);
    co_return co_await _stm.replicate_and_wait(
      make_batch<set_target_assignment_cmd>(std::move(v)), replicate_timeout);
}

kafka::consumer_group_heartbeat_response_data
coordinator::build_response(const kafka::group_id& gid, const member& m) {
    kafka::consumer_group_heartbeat_response_data resp;
    resp.member_epoch = m.current_member_epoch();
    resp.heartbeat_interval_ms = static_cast<int32_t>(
      _heartbeat_interval.count());

    // Build the assignment from the target assignment for this member.
    auto* group = _stm.find_group(gid);
    if (group) {
        kafka::consumer_group_heartbeat_assignment resp_assignment;
        for (const auto& tam : group->assignment.members) {
            if (tam.member_id == m.member_id) {
                for (const auto& tp : tam.partitions) {
                    kafka::consumer_group_heartbeat_assignment_topic_partitions
                      resp_tp;
                    resp_tp.topic_id = tp.topic_id;
                    for (const auto& pid : tp.partitions) {
                        resp_tp.partitions.push_back(pid);
                    }
                    resp_assignment.topic_partitions.push_back(
                      std::move(resp_tp));
                }
                break;
            }
        }
        resp.assignment = std::move(resp_assignment);
    }

    return resp;
}

kafka::described_group
coordinator::describe(const kafka::group_id& gid) {
    kafka::described_group grp;
    grp.group_id = gid;

    auto* group = _stm.find_group(gid);
    if (!group) {
        grp.error_code = kafka::error_code::group_id_not_found;
        return grp;
    }

    grp.group_epoch = group->epoch();
    grp.assignment_epoch = group->assignment.epoch();
    grp.assignor_name = group->assignor_name;

    switch (group->state) {
    case group_state::empty:
        grp.group_state = "Empty";
        break;
    case group_state::assigning:
        grp.group_state = "Assigning";
        break;
    case group_state::reconciling:
        grp.group_state = "Reconciling";
        break;
    case group_state::stable:
        grp.group_state = "Stable";
        break;
    case group_state::dead:
        grp.group_state = "Dead";
        break;
    }

    for (const auto& [mid, m] : group->members) {
        kafka::member resp_member;
        resp_member.member_id = m.member_id;
        resp_member.instance_id = m.instance_id;
        resp_member.rack_id = m.rack_id;
        resp_member.member_epoch = m.current_member_epoch();
        resp_member.client_id = m.client_id();
        resp_member.client_host = m.client_host();

        // Build current assignment.
        for (const auto& tp : m.assigned_partitions) {
            kafka::consumer_group_describe_assignment_topic_partitions resp_tp;
            resp_tp.topic_id = tp.topic_id;
            for (const auto& pid : tp.partitions) {
                resp_tp.partitions.push_back(pid);
            }
            resp_member.assignment.topic_partitions.push_back(
              std::move(resp_tp));
        }

        // Build target assignment.
        for (const auto& tam : group->assignment.members) {
            if (tam.member_id == m.member_id) {
                for (const auto& tp : tam.partitions) {
                    kafka::
                      consumer_group_describe_target_assignment_topic_partitions
                        resp_tp;
                    resp_tp.topic_id = tp.topic_id;
                    for (const auto& pid : tp.partitions) {
                        resp_tp.partitions.push_back(pid);
                    }
                    resp_member.target_assignment.topic_partitions.push_back(
                      std::move(resp_tp));
                }
                break;
            }
        }

        grp.members.push_back(std::move(resp_member));
    }

    return grp;
}

void coordinator::expire_sessions() {
    if (_gate.is_closed()) {
        return;
    }

    auto now = model::timestamp::now();
    auto timeout_ms = _session_timeout.count();

    for (const auto& [gid, group] : _stm.groups()) {
        for (const auto& [mid, m] : group.members) {
            auto elapsed = now.value() - m.last_heartbeat.value();
            if (elapsed > timeout_ms) {
                ssx::spawn_with_gate(
                  _gate, [this, gid = gid, mid = mid]() -> ss::future<> {
                      auto* grp = _stm.find_group(gid);
                      if (!grp) {
                          co_return;
                      }
                      auto it = grp->members.find(mid);
                      if (it == grp->members.end()) {
                          co_return;
                      }

                      remove_member_cmd::value v;
                      v.group_id = gid;
                      v.member_id = mid;
                      co_await _stm.replicate_and_wait(
                        make_batch<remove_member_cmd>(std::move(v)),
                        replicate_timeout);

                      co_await run_assignor(gid);
                  });
            }
        }
    }
}

} // namespace kafka::consumer_group
