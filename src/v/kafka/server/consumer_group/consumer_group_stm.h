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

#include "absl/container/node_hash_map.h"
#include "cluster/state_machine_registry.h"
#include "container/chunked_vector.h"
#include "kafka/server/consumer_group/commands.h"
#include "kafka/server/consumer_group/types.h"
#include "raft/persisted_stm.h"

namespace kafka::consumer_group {

/// Replicated state machine for KIP-848 consumer groups.
///
/// Manages consumer group state on __consumer_offsets partitions.
/// All mutations are replicated as commands through raft before being
/// applied to in-memory state.
class consumer_group_stm final : public raft::persisted_stm<> {
public:
    static constexpr std::string_view name = "consumer_group_stm";

    consumer_group_stm(ss::logger&, raft::consensus*);

    ss::future<> do_apply(const model::record_batch&) override;

    ss::future<raft::local_snapshot_applied>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&&) override;

    ss::future<raft::stm_snapshot>
      take_local_snapshot(ssx::semaphore_units) override;

    raft::stm_initial_recovery_policy
    get_initial_recovery_policy() const final {
        return raft::stm_initial_recovery_policy::read_everything;
    }

    ss::future<> apply_raft_snapshot(const iobuf&) final;
    ss::future<iobuf> take_raft_snapshot(model::offset) final;

    /// Replicate a command batch and wait for it to be applied.
    ss::future<std::error_code>
      replicate_and_wait(model::record_batch, model::timeout_clock::duration);

    /// Read-only access to group state.
    const consumer_group_data* find_group(const kafka::group_id&) const;

    const absl::node_hash_map<kafka::group_id, consumer_group_data>&
    groups() const {
        return _groups;
    }

    /// Snapshot type for serde serialization.
    struct snapshot
      : serde::envelope<snapshot, serde::version<0>, serde::compat_version<0>> {
        struct group_snapshot
          : serde::envelope<
              group_snapshot,
              serde::version<0>,
              serde::compat_version<0>> {
            kafka::group_id group_id;
            group_epoch epoch{0};
            group_state state{group_state::empty};
            ss::sstring assignor_name;
            chunked_vector<member> members;
            chunked_vector<topic_metadata> topics;
            target_assignment assignment;
            chunked_vector<committed_offset> offsets;

            auto serde_fields() {
                return std::tie(
                  group_id,
                  epoch,
                  state,
                  assignor_name,
                  members,
                  topics,
                  assignment,
                  offsets);
            }

            friend bool operator==(const group_snapshot&, const group_snapshot&)
              = default;
        };

        chunked_vector<group_snapshot> groups;

        auto serde_fields() { return std::tie(groups); }

        friend bool operator==(const snapshot&, const snapshot&) = default;
    };

private:
    void apply_update_group_metadata(update_group_metadata_cmd::value);
    void apply_upsert_member(upsert_member_cmd::value);
    void apply_remove_member(remove_member_cmd::value);
    void apply_update_topic_metadata(update_topic_metadata_cmd::value);
    void apply_set_target_assignment(set_target_assignment_cmd::value);
    void apply_update_member_assignment(update_member_assignment_cmd::value);
    void apply_commit_offset(commit_offset_cmd::value);
    void apply_delete_group(delete_group_cmd::value);

    consumer_group_data& get_or_create_group(const kafka::group_id&);

    absl::node_hash_map<kafka::group_id, consumer_group_data> _groups;
};

/// Factory for registering consumer_group_stm with the state machine
/// registry.
class consumer_group_stm_factory : public cluster::state_machine_factory {
public:
    consumer_group_stm_factory() = default;

    bool is_applicable_for(const storage::ntp_config&) const final;

    void create(
      raft::state_machine_manager_builder&,
      raft::consensus*,
      const cluster::stm_instance_config&) final;
};

} // namespace kafka::consumer_group
