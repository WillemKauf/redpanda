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

#include "kafka/server/consumer_group/commands.h"
#include "kafka/server/consumer_group/consumer_group_stm.h"
#include "kafka/server/consumer_group/types.h"
#include "model/record_batch_types.h"
#include "raft/tests/raft_fixture.h"
#include "raft/tests/raft_fixture_retry_policy.h"
#include "serde/rw/rw.h"
#include "storage/record_batch_builder.h"
#include "test_utils/test.h"

#include <seastar/core/coroutine.hh>

namespace kafka::consumer_group {

namespace {

ss::logger test_log("cg_stm_test");

template<typename Cmd>
model::record_batch make_batch(typename Cmd::value v) {
    storage::record_batch_builder builder(
      model::record_batch_type::consumer_group, model::offset{0});
    auto key_buf = serde::to_iobuf(Cmd::key);
    auto val_buf = serde::to_iobuf(std::move(v));
    builder.add_raw_kv(std::move(key_buf), std::move(val_buf));
    return std::move(builder).build();
}

} // namespace

struct consumer_group_stm_fixture : raft::stm_raft_fixture<consumer_group_stm> {
    static constexpr auto timeout = std::chrono::seconds{10};

    stm_shptrs_t create_stms(
      raft::state_machine_manager_builder& builder,
      raft::raft_node_instance& node) override {
        return builder.create_stm<consumer_group_stm>(
          test_log, node.raft().get());
    }

    ss::future<std::error_code> replicate(model::record_batch batch) {
        return stm_retry_with_leader<0>(
          timeout,
          [batch = std::move(batch)](
            const ss::shared_ptr<consumer_group_stm>& stm) mutable {
              return stm->replicate_and_wait(
                std::move(batch), std::chrono::seconds{5});
          });
    }

    const consumer_group_data* find_group(const kafka::group_id& gid) {
        auto& leader_node = node(*get_leader());
        auto stm = get_stm<0>(leader_node);
        return stm->find_group(gid);
    }
};

TEST_F_CORO(consumer_group_stm_fixture, UpsertAndFindMember) {
    co_await initialize_state_machines();

    kafka::group_id gid{"test-group"};

    member m;
    m.member_id = kafka::member_id{"m-0"};
    m.client_id = kafka::client_id{"client"};
    m.client_host = kafka::client_host{"/127.0.0.1"};
    m.server_assignor = "uniform";

    upsert_member_cmd::value v;
    v.group_id = gid;
    v.member_data = std::move(m);

    auto ec = co_await replicate(make_batch<upsert_member_cmd>(std::move(v)));
    ASSERT_EQ_CORO(ec, std::error_code{});

    auto* group = find_group(gid);
    ASSERT_NE_CORO(group, nullptr);
    ASSERT_EQ_CORO(group->members.size(), 1);
    ASSERT_TRUE_CORO(group->members.contains(kafka::member_id{"m-0"}));
}

TEST_F_CORO(consumer_group_stm_fixture, UpdateGroupMetadata) {
    co_await initialize_state_machines();

    kafka::group_id gid{"test-group"};

    update_group_metadata_cmd::value v;
    v.group_id = gid;
    v.epoch = group_epoch{1};
    v.state = group_state::stable;
    v.assignor_name = "uniform";

    auto ec = co_await replicate(
      make_batch<update_group_metadata_cmd>(std::move(v)));
    ASSERT_EQ_CORO(ec, std::error_code{});

    auto* group = find_group(gid);
    ASSERT_NE_CORO(group, nullptr);
    ASSERT_EQ_CORO(group->epoch(), 1);
    ASSERT_EQ_CORO(group->state, group_state::stable);
    ASSERT_EQ_CORO(group->assignor_name, "uniform");
}

TEST_F_CORO(consumer_group_stm_fixture, RemoveMember) {
    co_await initialize_state_machines();

    kafka::group_id gid{"test-group"};

    // Add a member.
    member m;
    m.member_id = kafka::member_id{"m-0"};
    m.client_id = kafka::client_id{"client"};
    m.client_host = kafka::client_host{"/127.0.0.1"};

    {
        upsert_member_cmd::value v;
        v.group_id = gid;
        v.member_data = std::move(m);
        auto ec = co_await replicate(
          make_batch<upsert_member_cmd>(std::move(v)));
        ASSERT_EQ_CORO(ec, std::error_code{});
    }

    // Remove the member.
    {
        remove_member_cmd::value v;
        v.group_id = gid;
        v.member_id = kafka::member_id{"m-0"};
        auto ec = co_await replicate(
          make_batch<remove_member_cmd>(std::move(v)));
        ASSERT_EQ_CORO(ec, std::error_code{});
    }

    auto* group = find_group(gid);
    ASSERT_NE_CORO(group, nullptr);
    ASSERT_TRUE_CORO(group->members.empty());
}

TEST_F_CORO(consumer_group_stm_fixture, DeleteGroup) {
    co_await initialize_state_machines();

    kafka::group_id gid{"test-group"};

    // Create a group.
    {
        update_group_metadata_cmd::value v;
        v.group_id = gid;
        v.epoch = group_epoch{1};
        v.state = group_state::stable;
        v.assignor_name = "uniform";
        auto ec = co_await replicate(
          make_batch<update_group_metadata_cmd>(std::move(v)));
        ASSERT_EQ_CORO(ec, std::error_code{});
    }

    auto* group = find_group(gid);
    ASSERT_NE_CORO(group, nullptr);

    // Delete the group.
    {
        delete_group_cmd::value v;
        v.group_id = gid;
        auto ec = co_await replicate(
          make_batch<delete_group_cmd>(std::move(v)));
        ASSERT_EQ_CORO(ec, std::error_code{});
    }

    group = find_group(gid);
    ASSERT_EQ_CORO(group, nullptr);
}

TEST_F_CORO(consumer_group_stm_fixture, SnapshotRoundTrip) {
    co_await initialize_state_machines();

    kafka::group_id gid{"test-group"};

    // Set up group with a member and metadata.
    {
        update_group_metadata_cmd::value v;
        v.group_id = gid;
        v.epoch = group_epoch{3};
        v.state = group_state::stable;
        v.assignor_name = "uniform";
        co_await replicate(make_batch<update_group_metadata_cmd>(std::move(v)));
    }

    member m;
    m.member_id = kafka::member_id{"m-0"};
    m.client_id = kafka::client_id{"client"};
    m.client_host = kafka::client_host{"/127.0.0.1"};
    m.server_assignor = "uniform";

    {
        upsert_member_cmd::value v;
        v.group_id = gid;
        v.member_data = std::move(m);
        co_await replicate(make_batch<upsert_member_cmd>(std::move(v)));
    }

    // Restart all nodes (forces log replay recovery).
    co_await restart_nodes();

    // Wait for STMs to catch up with the committed offset.
    auto committed_offset = co_await with_leader(
      timeout,
      [](raft::raft_node_instance& n) { return n.raft()->committed_offset(); });

    co_await parallel_for_each_node(
      [committed_offset](raft::raft_node_instance& n) {
          return n.raft()->stm_manager()->wait(
            committed_offset, model::timeout_clock::now() + 10s);
      });

    auto* group = find_group(gid);
    ASSERT_NE_CORO(group, nullptr);
    ASSERT_EQ_CORO(group->epoch(), 3);
    ASSERT_EQ_CORO(group->state, group_state::stable);
    ASSERT_EQ_CORO(group->members.size(), 1);
    ASSERT_TRUE_CORO(group->members.contains(kafka::member_id{"m-0"}));
}

} // namespace kafka::consumer_group
