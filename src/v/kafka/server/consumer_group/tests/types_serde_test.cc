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

#include "gtest/gtest.h"
#include "kafka/server/consumer_group/commands.h"
#include "kafka/server/consumer_group/types.h"
#include "serde/rw/rw.h"

namespace kafka::consumer_group {

namespace {

/// Serialize, deserialize twice, and compare the two decoded values.
/// Takes ownership of the value (avoids copy of non-copyable types).
template<typename T>
void serde_round_trip(T original) {
    auto buf = serde::to_iobuf(std::move(original));
    auto first = serde::from_iobuf<T>(buf.copy());
    auto second = serde::from_iobuf<T>(std::move(buf));
    EXPECT_EQ(first, second);
}

topic_partitions make_topic_partitions() {
    topic_partitions tp;
    tp.topic_id = model::topic_id::create();
    tp.partitions.push_back(model::partition_id{0});
    tp.partitions.push_back(model::partition_id{1});
    return tp;
}

member make_test_member() {
    member m;
    m.member_id = kafka::member_id{"member-0"};
    m.instance_id = kafka::group_instance_id{"instance-0"};
    m.rack_id = "rack-a";
    m.client_id = kafka::client_id{"client-0"};
    m.client_host = kafka::client_host{"/127.0.0.1"};
    m.subscribed_topic_ids.push_back(model::topic_id::create());
    m.server_assignor = "uniform";
    m.current_member_epoch = member_epoch{1};
    m.previous_member_epoch = member_epoch{0};
    m.assignment_state = member_assignment_state::stable;
    m.assigned_partitions.push_back(make_topic_partitions());
    m.session_timeout = std::chrono::milliseconds{45000};
    m.rebalance_timeout = std::chrono::milliseconds{300000};
    return m;
}

} // namespace

TEST(TypesSerdeTest, TopicPartitionsRoundTrip) {
    serde_round_trip(make_topic_partitions());
}

TEST(TypesSerdeTest, TopicMetadataRoundTrip) {
    topic_metadata tm;
    tm.topic_id = model::topic_id::create();
    tm.topic_name = model::topic{"test-topic"};
    tm.num_partitions = 12;
    serde_round_trip(std::move(tm));
}

TEST(TypesSerdeTest, MemberRoundTrip) { serde_round_trip(make_test_member()); }

TEST(TypesSerdeTest, MemberWithoutOptionals) {
    member m;
    m.member_id = kafka::member_id{"member-1"};
    m.client_id = kafka::client_id{"client-1"};
    m.client_host = kafka::client_host{"/10.0.0.1"};
    m.server_assignor = "range";
    serde_round_trip(std::move(m));
}

TEST(TypesSerdeTest, TargetAssignmentRoundTrip) {
    target_assignment_member tam;
    tam.member_id = kafka::member_id{"member-0"};
    tam.partitions.push_back(make_topic_partitions());

    target_assignment ta;
    ta.epoch = assignment_epoch{3};
    ta.members.push_back(std::move(tam));
    serde_round_trip(std::move(ta));
}

TEST(TypesSerdeTest, CommittedOffsetRoundTrip) {
    committed_offset co;
    co.topic = model::topic{"test-topic"};
    co.partition = model::partition_id{5};
    co.offset = kafka::offset{42};
    co.leader_epoch = kafka::leader_epoch{1};
    co.metadata = "some-metadata";
    co.commit_timestamp = model::timestamp{1234567890};
    serde_round_trip(std::move(co));
}

TEST(TypesSerdeTest, CommittedOffsetNoMetadata) {
    committed_offset co;
    co.topic = model::topic{"test-topic"};
    co.partition = model::partition_id{0};
    co.offset = kafka::offset{100};
    co.leader_epoch = kafka::invalid_leader_epoch;
    co.commit_timestamp = model::timestamp{0};
    serde_round_trip(std::move(co));
}

// Command serde tests

TEST(CommandsSerdeTest, UpdateGroupMetadataCmd) {
    update_group_metadata_cmd::value v;
    v.group_id = kafka::group_id{"test-group"};
    v.epoch = group_epoch{5};
    v.state = group_state::reconciling;
    v.assignor_name = "uniform";
    serde_round_trip(std::move(v));
}

TEST(CommandsSerdeTest, UpsertMemberCmd) {
    upsert_member_cmd::value v;
    v.group_id = kafka::group_id{"test-group"};
    v.member_data = make_test_member();
    serde_round_trip(std::move(v));
}

TEST(CommandsSerdeTest, RemoveMemberCmd) {
    remove_member_cmd::value v;
    v.group_id = kafka::group_id{"test-group"};
    v.member_id = kafka::member_id{"member-0"};
    serde_round_trip(std::move(v));
}

TEST(CommandsSerdeTest, UpdateTopicMetadataCmd) {
    update_topic_metadata_cmd::value v;
    v.group_id = kafka::group_id{"test-group"};
    topic_metadata tm;
    tm.topic_id = model::topic_id::create();
    tm.topic_name = model::topic{"t"};
    tm.num_partitions = 3;
    v.topics.push_back(std::move(tm));
    serde_round_trip(std::move(v));
}

TEST(CommandsSerdeTest, SetTargetAssignmentCmd) {
    target_assignment_member tam;
    tam.member_id = kafka::member_id{"m-0"};
    tam.partitions.push_back(make_topic_partitions());

    target_assignment ta;
    ta.epoch = assignment_epoch{1};
    ta.members.push_back(std::move(tam));

    set_target_assignment_cmd::value v;
    v.group_id = kafka::group_id{"test-group"};
    v.assignment = std::move(ta);
    serde_round_trip(std::move(v));
}

TEST(CommandsSerdeTest, UpdateMemberAssignmentCmd) {
    update_member_assignment_cmd::value v;
    v.group_id = kafka::group_id{"test-group"};
    v.member_id = kafka::member_id{"member-0"};
    v.epoch = member_epoch{2};
    v.state = member_assignment_state::revoking;
    v.assigned_partitions.push_back(make_topic_partitions());
    v.revoking_partitions.push_back(make_topic_partitions());
    serde_round_trip(std::move(v));
}

TEST(CommandsSerdeTest, CommitOffsetCmd) {
    committed_offset co;
    co.topic = model::topic{"t"};
    co.partition = model::partition_id{0};
    co.offset = kafka::offset{99};
    co.leader_epoch = kafka::leader_epoch{0};
    co.commit_timestamp = model::timestamp::now();

    commit_offset_cmd::value v;
    v.group_id = kafka::group_id{"test-group"};
    v.offsets.push_back(std::move(co));
    serde_round_trip(std::move(v));
}

TEST(CommandsSerdeTest, DeleteGroupCmd) {
    delete_group_cmd::value v;
    v.group_id = kafka::group_id{"test-group"};
    serde_round_trip(std::move(v));
}

TEST(CommandsSerdeTest, CmdKeyRoundTrip) {
    for (uint8_t i = 0; i <= 7; ++i) {
        auto key = cmd_key{i};
        auto buf = serde::to_iobuf(key);
        auto decoded = serde::from_iobuf<cmd_key>(std::move(buf));
        EXPECT_EQ(key, decoded);
    }
}

} // namespace kafka::consumer_group
