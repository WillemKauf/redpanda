// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/consumer_group_assignor.h"
#include "kafka/server/consumer_group_member.h"
#include "kafka/server/consumer_group_state.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

namespace kafka {

// Helper to create a test member.
static consumer_group_member_ptr make_member(
  ss::sstring id,
  chunked_vector<ss::sstring> topics,
  std::optional<kafka::group_instance_id> instance_id = std::nullopt) {
    return ss::make_lw_shared<consumer_group_member>(
      kafka::member_id(std::move(id)),
      std::move(instance_id),
      std::nullopt,
      std::chrono::milliseconds(30000),
      std::move(topics),
      std::nullopt,
      std::nullopt);
}

// --- consumer_group_member tests ---

SEASTAR_THREAD_TEST_CASE(member_basic_properties) {
    chunked_vector<ss::sstring> topics;
    topics.push_back("topic-a");
    topics.push_back("topic-b");

    auto member = make_member("member-1", std::move(topics));

    BOOST_TEST(member->id() == kafka::member_id("member-1"));
    BOOST_TEST(!member->instance_id().has_value());
    BOOST_TEST(!member->rack_id().has_value());
    BOOST_TEST(
      member->rebalance_timeout() == std::chrono::milliseconds(30000));
    BOOST_TEST(member->member_epoch() == kafka::consumer_group_member_epoch(0));
    BOOST_TEST(member->subscribed_topic_names().size() == 2);
    BOOST_TEST(member->subscribed_topic_names()[0] == "topic-a");
    BOOST_TEST(member->subscribed_topic_names()[1] == "topic-b");
}

SEASTAR_THREAD_TEST_CASE(member_epoch_updates) {
    auto member = make_member("m1", {});

    BOOST_TEST(member->member_epoch() == kafka::consumer_group_member_epoch(0));
    member->set_member_epoch(kafka::consumer_group_member_epoch(5));
    BOOST_TEST(member->member_epoch() == kafka::consumer_group_member_epoch(5));
}

SEASTAR_THREAD_TEST_CASE(member_assignment_tracking) {
    auto member = make_member("m1", {});

    // Initially no assignments.
    BOOST_TEST(member->target_assignment().empty());
    BOOST_TEST(member->current_assignment().empty());
    BOOST_TEST(member->is_at_target());

    // Set target assignment.
    consumer_group_member::assignment_type target;
    target[model::topic_id(uuid_t::create())].push_back(
      model::partition_id(0));
    member->set_target_assignment(std::move(target));

    BOOST_TEST(!member->target_assignment().empty());
    BOOST_TEST(!member->is_at_target());

    // Set current to match target.
    consumer_group_member::assignment_type current;
    for (const auto& [tid, parts] : member->target_assignment()) {
        current[tid] = parts;
    }
    member->set_current_assignment(std::move(current));

    BOOST_TEST(member->is_at_target());
}

SEASTAR_THREAD_TEST_CASE(member_static_instance) {
    auto instance = kafka::group_instance_id("static-1");
    chunked_vector<ss::sstring> topics;
    topics.push_back("t1");
    auto member = make_member("m1", std::move(topics), instance);

    BOOST_TEST(member->instance_id().has_value());
    BOOST_TEST(*member->instance_id() == instance);
}

SEASTAR_THREAD_TEST_CASE(member_subscription_update) {
    chunked_vector<ss::sstring> topics;
    topics.push_back("old-topic");
    auto member = make_member("m1", std::move(topics));

    BOOST_TEST(member->subscribed_topic_names().size() == 1);
    BOOST_TEST(member->subscribed_topic_names()[0] == "old-topic");

    chunked_vector<ss::sstring> new_topics;
    new_topics.push_back("new-topic-a");
    new_topics.push_back("new-topic-b");
    member->set_subscribed_topic_names(std::move(new_topics));

    BOOST_TEST(member->subscribed_topic_names().size() == 2);
    BOOST_TEST(member->subscribed_topic_names()[0] == "new-topic-a");
}

// --- consumer_group_state tests ---

SEASTAR_THREAD_TEST_CASE(state_to_string) {
    BOOST_TEST(
      consumer_group_state_to_string(consumer_group_state::empty) == "Empty");
    BOOST_TEST(
      consumer_group_state_to_string(consumer_group_state::assigning)
      == "Assigning");
    BOOST_TEST(
      consumer_group_state_to_string(consumer_group_state::reconciling)
      == "Reconciling");
    BOOST_TEST(
      consumer_group_state_to_string(consumer_group_state::stable)
      == "Stable");
    BOOST_TEST(
      consumer_group_state_to_string(consumer_group_state::dead) == "Dead");
}

// --- uniform_assignor tests ---

SEASTAR_THREAD_TEST_CASE(assignor_name) {
    uniform_assignor assignor;
    BOOST_TEST(assignor.name() == kafka::server_assignor("uniform"));
}

SEASTAR_THREAD_TEST_CASE(assignor_empty_members) {
    uniform_assignor assignor;
    std::vector<assignable_topic> topics;
    absl::node_hash_map<kafka::member_id, consumer_group_member_ptr> members;

    auto result = assignor.assign(topics, members);
    BOOST_TEST(result.empty());
}

SEASTAR_THREAD_TEST_CASE(assignor_single_member_single_topic) {
    uniform_assignor assignor;

    auto topic_id = model::topic_id(uuid_t::create());
    std::vector<assignable_topic> topics;
    topics.push_back(
      assignable_topic{.id = topic_id, .name = "t1", .partition_count = 3});

    chunked_vector<ss::sstring> sub;
    sub.push_back("t1");
    absl::node_hash_map<kafka::member_id, consumer_group_member_ptr> members;
    members[kafka::member_id("m1")] = make_member("m1", std::move(sub));

    auto result = assignor.assign(topics, members);

    BOOST_TEST(result.size() == 1);
    auto it = result.find(kafka::member_id("m1"));
    BOOST_REQUIRE(it != result.end());

    // m1 should get all 3 partitions.
    auto tp_it = it->second.find(topic_id);
    BOOST_REQUIRE(tp_it != it->second.end());
    BOOST_TEST(tp_it->second.size() == 3);
}

SEASTAR_THREAD_TEST_CASE(assignor_two_members_even_distribution) {
    uniform_assignor assignor;

    auto topic_id = model::topic_id(uuid_t::create());
    std::vector<assignable_topic> topics;
    topics.push_back(
      assignable_topic{.id = topic_id, .name = "t1", .partition_count = 4});

    chunked_vector<ss::sstring> sub1;
    sub1.push_back("t1");
    chunked_vector<ss::sstring> sub2;
    sub2.push_back("t1");

    absl::node_hash_map<kafka::member_id, consumer_group_member_ptr> members;
    members[kafka::member_id("m1")] = make_member("m1", std::move(sub1));
    members[kafka::member_id("m2")] = make_member("m2", std::move(sub2));

    auto result = assignor.assign(topics, members);

    BOOST_TEST(result.size() == 2);

    size_t total_partitions = 0;
    for (const auto& [_, assignment] : result) {
        auto tp_it = assignment.find(topic_id);
        if (tp_it != assignment.end()) {
            total_partitions += tp_it->second.size();
            // Each member should get 2 partitions (4/2).
            BOOST_TEST(tp_it->second.size() == 2);
        }
    }
    BOOST_TEST(total_partitions == 4);
}

SEASTAR_THREAD_TEST_CASE(assignor_uneven_distribution) {
    uniform_assignor assignor;

    auto topic_id = model::topic_id(uuid_t::create());
    std::vector<assignable_topic> topics;
    topics.push_back(
      assignable_topic{.id = topic_id, .name = "t1", .partition_count = 5});

    chunked_vector<ss::sstring> sub1;
    sub1.push_back("t1");
    chunked_vector<ss::sstring> sub2;
    sub2.push_back("t1");

    absl::node_hash_map<kafka::member_id, consumer_group_member_ptr> members;
    members[kafka::member_id("m1")] = make_member("m1", std::move(sub1));
    members[kafka::member_id("m2")] = make_member("m2", std::move(sub2));

    auto result = assignor.assign(topics, members);

    // 5 partitions across 2 members: one gets 3, other gets 2.
    size_t total = 0;
    size_t max_per_member = 0;
    size_t min_per_member = 100;
    for (const auto& [_, assignment] : result) {
        auto tp_it = assignment.find(topic_id);
        if (tp_it != assignment.end()) {
            total += tp_it->second.size();
            max_per_member = std::max(max_per_member, tp_it->second.size());
            min_per_member = std::min(min_per_member, tp_it->second.size());
        }
    }
    BOOST_TEST(total == 5);
    BOOST_TEST(max_per_member == 3);
    BOOST_TEST(min_per_member == 2);
}

SEASTAR_THREAD_TEST_CASE(assignor_partial_subscription) {
    uniform_assignor assignor;

    auto tid1 = model::topic_id(uuid_t::create());
    auto tid2 = model::topic_id(uuid_t::create());
    std::vector<assignable_topic> topics;
    topics.push_back(
      assignable_topic{.id = tid1, .name = "t1", .partition_count = 2});
    topics.push_back(
      assignable_topic{.id = tid2, .name = "t2", .partition_count = 2});

    // m1 subscribes to t1 only, m2 subscribes to both.
    chunked_vector<ss::sstring> sub1;
    sub1.push_back("t1");
    chunked_vector<ss::sstring> sub2;
    sub2.push_back("t1");
    sub2.push_back("t2");

    absl::node_hash_map<kafka::member_id, consumer_group_member_ptr> members;
    members[kafka::member_id("m1")] = make_member("m1", std::move(sub1));
    members[kafka::member_id("m2")] = make_member("m2", std::move(sub2));

    auto result = assignor.assign(topics, members);

    // t1: split between m1 and m2 (1 each)
    // t2: all to m2 (only subscriber)
    auto m2_it = result.find(kafka::member_id("m2"));
    BOOST_REQUIRE(m2_it != result.end());

    auto t2_it = m2_it->second.find(tid2);
    BOOST_REQUIRE(t2_it != m2_it->second.end());
    BOOST_TEST(t2_it->second.size() == 2);
}

SEASTAR_THREAD_TEST_CASE(assignor_no_subscribers_for_topic) {
    uniform_assignor assignor;

    auto topic_id = model::topic_id(uuid_t::create());
    std::vector<assignable_topic> topics;
    topics.push_back(
      assignable_topic{.id = topic_id, .name = "t1", .partition_count = 3});

    // Member subscribes to a different topic.
    chunked_vector<ss::sstring> sub;
    sub.push_back("other-topic");

    absl::node_hash_map<kafka::member_id, consumer_group_member_ptr> members;
    members[kafka::member_id("m1")] = make_member("m1", std::move(sub));

    auto result = assignor.assign(topics, members);

    // m1 should have an empty assignment for this topic.
    auto m1_it = result.find(kafka::member_id("m1"));
    BOOST_REQUIRE(m1_it != result.end());
    BOOST_TEST(m1_it->second.empty());
}

// --- Additional member tests ---

SEASTAR_THREAD_TEST_CASE(member_heartbeat_tracking) {
    auto member = make_member("m1", {});

    auto before = ss::lowres_clock::now();
    member->set_latest_heartbeat(before);
    BOOST_CHECK(member->latest_heartbeat() == before);

    auto after = before + std::chrono::seconds(5);
    member->set_latest_heartbeat(after);
    BOOST_CHECK(member->latest_heartbeat() == after);
}

SEASTAR_THREAD_TEST_CASE(member_server_assignor) {
    auto member = ss::make_lw_shared<consumer_group_member>(
      kafka::member_id("m1"),
      std::nullopt,
      std::nullopt,
      std::chrono::milliseconds(30000),
      chunked_vector<ss::sstring>{},
      std::nullopt,
      kafka::server_assignor("range"));

    BOOST_TEST(member->server_assignor().has_value());
    BOOST_TEST(*member->server_assignor() == kafka::server_assignor("range"));
}

SEASTAR_THREAD_TEST_CASE(member_rack_id) {
    auto member = ss::make_lw_shared<consumer_group_member>(
      kafka::member_id("m1"),
      std::nullopt,
      ss::sstring("us-east-1"),
      std::chrono::milliseconds(30000),
      chunked_vector<ss::sstring>{},
      std::nullopt,
      std::nullopt);

    BOOST_TEST(member->rack_id().has_value());
    BOOST_TEST(*member->rack_id() == "us-east-1");
}

// --- Additional assignor tests ---

SEASTAR_THREAD_TEST_CASE(assignor_multiple_topics_multiple_members) {
    uniform_assignor assignor;

    auto tid1 = model::topic_id(uuid_t::create());
    auto tid2 = model::topic_id(uuid_t::create());
    auto tid3 = model::topic_id(uuid_t::create());

    std::vector<assignable_topic> topics;
    topics.push_back(
      assignable_topic{.id = tid1, .name = "t1", .partition_count = 6});
    topics.push_back(
      assignable_topic{.id = tid2, .name = "t2", .partition_count = 3});
    topics.push_back(
      assignable_topic{.id = tid3, .name = "t3", .partition_count = 1});

    // All three members subscribe to all three topics.
    absl::node_hash_map<kafka::member_id, consumer_group_member_ptr> members;
    for (int i = 1; i <= 3; ++i) {
        auto id = fmt::format("m{}", i);
        chunked_vector<ss::sstring> sub;
        sub.push_back("t1");
        sub.push_back("t2");
        sub.push_back("t3");
        members[kafka::member_id(id)] = make_member(id, std::move(sub));
    }

    auto result = assignor.assign(topics, members);
    BOOST_TEST(result.size() == 3);

    // Count total partitions assigned
    size_t total = 0;
    for (const auto& [_, assignment] : result) {
        for (const auto& [__, parts] : assignment) {
            total += parts.size();
        }
    }
    // 6 + 3 + 1 = 10 total partitions
    BOOST_TEST(total == 10);
}

SEASTAR_THREAD_TEST_CASE(assignor_single_partition_topic) {
    uniform_assignor assignor;

    auto tid = model::topic_id(uuid_t::create());
    std::vector<assignable_topic> topics;
    topics.push_back(
      assignable_topic{.id = tid, .name = "t1", .partition_count = 1});

    // Three members, but only one partition.
    absl::node_hash_map<kafka::member_id, consumer_group_member_ptr> members;
    for (int i = 1; i <= 3; ++i) {
        auto id = fmt::format("m{}", i);
        chunked_vector<ss::sstring> sub;
        sub.push_back("t1");
        members[kafka::member_id(id)] = make_member(id, std::move(sub));
    }

    auto result = assignor.assign(topics, members);

    // Only one member should get the single partition.
    size_t members_with_partitions = 0;
    for (const auto& [_, assignment] : result) {
        auto tp_it = assignment.find(tid);
        if (tp_it != assignment.end() && !tp_it->second.empty()) {
            members_with_partitions++;
            BOOST_TEST(tp_it->second.size() == 1);
        }
    }
    BOOST_TEST(members_with_partitions == 1);
}

} // namespace kafka
