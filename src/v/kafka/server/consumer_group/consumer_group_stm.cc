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

#include "kafka/server/consumer_group/consumer_group_stm.h"

#include "kafka/server/logger.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "raft/consensus.h"
#include "serde/rw/rw.h"

namespace kafka::consumer_group {

consumer_group_stm::consumer_group_stm(
  ss::logger& logger, raft::consensus* raft)
  : raft::persisted_stm<>(ss::sstring(name), logger, raft) {}

ss::future<> consumer_group_stm::do_apply(const model::record_batch& b) {
    if (b.header().type != model::record_batch_type::consumer_group) {
        co_return;
    }

    b.for_each_record([this](model::record r) {
        auto key = serde::from_iobuf<cmd_key>(r.release_key());

        switch (key) {
        case update_group_metadata_cmd::key:
            apply_update_group_metadata(
              serde::from_iobuf<update_group_metadata_cmd::value>(
                r.release_value()));
            break;
        case upsert_member_cmd::key:
            apply_upsert_member(
              serde::from_iobuf<upsert_member_cmd::value>(
                r.release_value()));
            break;
        case remove_member_cmd::key:
            apply_remove_member(
              serde::from_iobuf<remove_member_cmd::value>(
                r.release_value()));
            break;
        case update_topic_metadata_cmd::key:
            apply_update_topic_metadata(
              serde::from_iobuf<update_topic_metadata_cmd::value>(
                r.release_value()));
            break;
        case set_target_assignment_cmd::key:
            apply_set_target_assignment(
              serde::from_iobuf<set_target_assignment_cmd::value>(
                r.release_value()));
            break;
        case update_member_assignment_cmd::key:
            apply_update_member_assignment(
              serde::from_iobuf<update_member_assignment_cmd::value>(
                r.release_value()));
            break;
        case commit_offset_cmd::key:
            apply_commit_offset(
              serde::from_iobuf<commit_offset_cmd::value>(
                r.release_value()));
            break;
        case delete_group_cmd::key:
            apply_delete_group(
              serde::from_iobuf<delete_group_cmd::value>(
                r.release_value()));
            break;
        default:
            vlog(
              cg_klog.warn,
              "consumer_group_stm: unknown command key: {}",
              key());
            break;
        }
    });
}

ss::future<raft::local_snapshot_applied>
consumer_group_stm::apply_local_snapshot(
  raft::stm_snapshot_header, iobuf&& data) {
    auto snap = serde::from_iobuf<snapshot>(std::move(data));

    _groups.clear();
    for (auto& gs : snap.groups) {
        consumer_group_data group;
        group.group_id = gs.group_id;
        group.epoch = gs.epoch;
        group.state = gs.state;
        group.assignor_name = std::move(gs.assignor_name);
        group.topics = std::move(gs.topics);
        group.assignment = std::move(gs.assignment);

        for (auto& m : gs.members) {
            auto mid = m.member_id;
            group.members.emplace(mid, std::move(m));
        }

        for (auto& co : gs.offsets) {
            offset_key ok{co.topic, co.partition};
            group.offsets.emplace(ok, std::move(co));
        }

        _groups.emplace(std::move(gs.group_id), std::move(group));
    }

    co_return raft::local_snapshot_applied::yes;
}

ss::future<raft::stm_snapshot>
consumer_group_stm::take_local_snapshot(ssx::semaphore_units) {
    snapshot snap;
    snap.groups.reserve(_groups.size());

    for (const auto& [gid, group] : _groups) {
        snapshot::group_snapshot gs;
        gs.group_id = gid;
        gs.epoch = group.epoch;
        gs.state = group.state;
        gs.assignor_name = group.assignor_name;
        gs.topics = group.topics.copy();
        gs.assignment = group.assignment.copy();

        gs.members.reserve(group.members.size());
        for (const auto& [_, m] : group.members) {
            gs.members.push_back(m.copy());
        }

        gs.offsets.reserve(group.offsets.size());
        for (const auto& [_, co] : group.offsets) {
            gs.offsets.push_back(co);
        }

        snap.groups.push_back(std::move(gs));
    }

    iobuf snap_data = serde::to_iobuf(std::move(snap));
    co_return raft::stm_snapshot::create(
      0, last_applied_offset(), std::move(snap_data));
}

ss::future<> consumer_group_stm::apply_raft_snapshot(const iobuf& buf) {
    auto snap = serde::from_iobuf<snapshot>(buf.copy());
    _groups.clear();
    for (auto& gs : snap.groups) {
        consumer_group_data group;
        group.group_id = gs.group_id;
        group.epoch = gs.epoch;
        group.state = gs.state;
        group.assignor_name = std::move(gs.assignor_name);
        group.topics = std::move(gs.topics);
        group.assignment = std::move(gs.assignment);

        for (auto& m : gs.members) {
            auto mid = m.member_id;
            group.members.emplace(mid, std::move(m));
        }

        for (auto& co : gs.offsets) {
            offset_key ok{co.topic, co.partition};
            group.offsets.emplace(ok, std::move(co));
        }

        _groups.emplace(std::move(gs.group_id), std::move(group));
    }
    co_return;
}

ss::future<iobuf>
consumer_group_stm::take_raft_snapshot(model::offset /*last_included*/) {
    // Reuse the same snapshot format.
    auto snap_result = co_await take_local_snapshot(ssx::semaphore_units{});
    co_return std::move(snap_result.data);
}

ss::future<std::error_code> consumer_group_stm::replicate_and_wait(
  model::record_batch batch, model::timeout_clock::duration timeout) {
    auto opts = raft::replicate_options(raft::consistency_level::quorum_ack);
    opts.set_force_flush();

    auto result = co_await _raft->replicate(std::move(batch), opts);
    if (!result) {
        co_return result.error();
    }

    auto applied = co_await wait_no_throw(
      result.value().last_offset,
      model::timeout_clock::now() + timeout);
    if (!applied) {
        co_return raft::errc::timeout;
    }

    co_return std::error_code{};
}

const consumer_group_data*
consumer_group_stm::find_group(const kafka::group_id& gid) const {
    auto it = _groups.find(gid);
    if (it == _groups.end()) {
        return nullptr;
    }
    return &it->second;
}

consumer_group_data&
consumer_group_stm::get_or_create_group(const kafka::group_id& gid) {
    auto [it, _] = _groups.try_emplace(gid);
    if (it->second.group_id == kafka::group_id{""}) {
        it->second.group_id = gid;
    }
    return it->second;
}

void consumer_group_stm::apply_update_group_metadata(
  update_group_metadata_cmd::value v) {
    auto& group = get_or_create_group(v.group_id);
    group.epoch = v.epoch;
    group.state = v.state;
    group.assignor_name = std::move(v.assignor_name);
}

void consumer_group_stm::apply_upsert_member(upsert_member_cmd::value v) {
    auto& group = get_or_create_group(v.group_id);
    auto mid = v.member_data.member_id;
    group.members.insert_or_assign(mid, std::move(v.member_data));
}

void consumer_group_stm::apply_remove_member(remove_member_cmd::value v) {
    auto it = _groups.find(v.group_id);
    if (it == _groups.end()) {
        return;
    }
    it->second.members.erase(v.member_id);
}

void consumer_group_stm::apply_update_topic_metadata(
  update_topic_metadata_cmd::value v) {
    auto& group = get_or_create_group(v.group_id);
    group.topics = std::move(v.topics);
}

void consumer_group_stm::apply_set_target_assignment(
  set_target_assignment_cmd::value v) {
    auto& group = get_or_create_group(v.group_id);
    group.assignment = std::move(v.assignment);
}

void consumer_group_stm::apply_update_member_assignment(
  update_member_assignment_cmd::value v) {
    auto it = _groups.find(v.group_id);
    if (it == _groups.end()) {
        return;
    }
    auto mit = it->second.members.find(v.member_id);
    if (mit == it->second.members.end()) {
        return;
    }
    auto& m = mit->second;
    m.current_member_epoch = v.epoch;
    m.assignment_state = v.state;
    m.assigned_partitions = std::move(v.assigned_partitions);
    m.revoking_partitions = std::move(v.revoking_partitions);
    m.pending_partitions = std::move(v.pending_partitions);
}

void consumer_group_stm::apply_commit_offset(commit_offset_cmd::value v) {
    auto& group = get_or_create_group(v.group_id);
    for (auto& co : v.offsets) {
        offset_key ok{co.topic, co.partition};
        group.offsets.insert_or_assign(ok, std::move(co));
    }
}

void consumer_group_stm::apply_delete_group(delete_group_cmd::value v) {
    _groups.erase(v.group_id);
}

// Factory implementation

bool consumer_group_stm_factory::is_applicable_for(
  const storage::ntp_config& config) const {
    const auto& ntp = config.ntp();
    return ntp.ns == model::kafka_consumer_offsets_nt.ns
           && ntp.tp.topic == model::kafka_consumer_offsets_nt.tp;
}

void consumer_group_stm_factory::create(
  raft::state_machine_manager_builder& builder,
  raft::consensus* raft,
  const cluster::stm_instance_config&) {
    auto stm = builder.create_stm<consumer_group_stm>(cg_klog, raft);
    raft->log()->stm_manager()->add_stm(stm);
}

} // namespace kafka::consumer_group
