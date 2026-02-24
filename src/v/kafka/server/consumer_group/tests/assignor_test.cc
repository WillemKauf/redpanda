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

#include "container/chunked_vector.h"
#include "gtest/gtest.h"
#include "kafka/server/consumer_group/assignor.h"
#include "kafka/server/consumer_group/types.h"

namespace kafka::consumer_group {

namespace {

topic_metadata make_topic(int32_t num_partitions) {
    topic_metadata tm;
    tm.topic_id = model::topic_id::create();
    tm.topic_name = model::topic{"test-topic"};
    tm.num_partitions = num_partitions;
    return tm;
}

member make_member(
  const char* id, const std::vector<model::topic_id>& subscriptions) {
    member m;
    m.member_id = kafka::member_id{id};
    m.client_id = kafka::client_id{"client"};
    m.client_host = kafka::client_host{"/127.0.0.1"};
    m.server_assignor = "uniform";
    for (const auto& tid : subscriptions) {
        m.subscribed_topic_ids.push_back(tid);
    }
    return m;
}

void add_member(
  absl::node_hash_map<kafka::member_id, member>& members, member m) {
    auto mid = m.member_id;
    members.emplace(std::move(mid), std::move(m));
}

} // namespace

TEST(AssignorTest, EmptyMembers) {
    absl::node_hash_map<kafka::member_id, member> members;
    chunked_vector<topic_metadata> topics;
    topics.push_back(make_topic(3));

    auto result = compute_target_assignment(
      assignment_epoch{1}, members, topics);

    EXPECT_EQ(result.epoch(), 1);
    EXPECT_TRUE(result.members.empty());
}

TEST(AssignorTest, SingleMemberSingleTopic) {
    auto topic = make_topic(3);
    auto tid = topic.topic_id;

    absl::node_hash_map<kafka::member_id, member> members;
    add_member(members, make_member("m-0", {tid}));

    chunked_vector<topic_metadata> topics;
    topics.push_back(std::move(topic));

    auto result = compute_target_assignment(
      assignment_epoch{1}, members, topics);

    EXPECT_EQ(result.members.size(), 1);
    EXPECT_EQ(result.members[0].member_id, kafka::member_id{"m-0"});
    EXPECT_EQ(result.members[0].partitions.size(), 1);
    EXPECT_EQ(result.members[0].partitions[0].topic_id, tid);
    EXPECT_EQ(result.members[0].partitions[0].partitions.size(), 3);
}

TEST(AssignorTest, TwoMembersEvenDistribution) {
    auto topic = make_topic(4);
    auto tid = topic.topic_id;

    absl::node_hash_map<kafka::member_id, member> members;
    add_member(members, make_member("m-0", {tid}));
    add_member(members, make_member("m-1", {tid}));

    chunked_vector<topic_metadata> topics;
    topics.push_back(std::move(topic));

    auto result = compute_target_assignment(
      assignment_epoch{1}, members, topics);

    EXPECT_EQ(result.members.size(), 2);

    // Both members should get 2 partitions each (round-robin).
    size_t total = 0;
    for (const auto& tam : result.members) {
        for (const auto& tp : tam.partitions) {
            total += tp.partitions.size();
        }
    }
    EXPECT_EQ(total, 4);
}

TEST(AssignorTest, UnsubscribedMemberGetsNothing) {
    auto topic = make_topic(3);
    auto subscribed_topic_id = topic.topic_id;
    auto other_topic_id = model::topic_id::create();

    absl::node_hash_map<kafka::member_id, member> members;
    add_member(members, make_member("m-0", {subscribed_topic_id}));
    add_member(members, make_member("m-1", {other_topic_id}));

    chunked_vector<topic_metadata> topics;
    topics.push_back(std::move(topic));

    auto result = compute_target_assignment(
      assignment_epoch{1}, members, topics);

    EXPECT_EQ(result.members.size(), 2);

    // Find m-0's assignment — should have all 3 partitions.
    for (const auto& tam : result.members) {
        if (tam.member_id == kafka::member_id{"m-0"}) {
            size_t total = 0;
            for (const auto& tp : tam.partitions) {
                total += tp.partitions.size();
            }
            EXPECT_EQ(total, 3);
        } else {
            // m-1 should have no partitions for this topic.
            EXPECT_TRUE(tam.partitions.empty());
        }
    }
}

} // namespace kafka::consumer_group
