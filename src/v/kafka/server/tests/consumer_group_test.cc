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
#include "kafka/server/consumer_group_probe.h"
#include "kafka/server/consumer_group_state.h"
#include "kafka/server/group_metadata.h"
#include "storage/record_batch_builder.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

#include <set>

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
    BOOST_TEST(member->rebalance_timeout() == std::chrono::milliseconds(30000));
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
    target[model::topic_id(uuid_t::create())].push_back(model::partition_id(0));
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
      consumer_group_state_to_string(consumer_group_state::stable) == "Stable");
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

// --- consumer_group_metadata serialization tests ---

SEASTAR_THREAD_TEST_CASE(metadata_member_state_encode_decode_roundtrip) {
    consumer_group_member_state original;
    original.id = kafka::member_id("m-123");
    original.instance_id = kafka::group_instance_id("inst-1");
    original.rack_id = "us-east-1";
    original.rebalance_timeout = std::chrono::milliseconds(45000);
    original.member_epoch = 7;
    original.subscribed_topic_names = {"topic-a", "topic-b", "topic-c"};
    original.subscribed_topic_regex = "topic-.*";

    iobuf buf;
    protocol::encoder writer(buf);
    consumer_group_member_state::encode(writer, original);

    auto reader = protocol::decoder(std::move(buf));
    auto decoded = consumer_group_member_state::decode(reader);

    BOOST_TEST(decoded.id == original.id);
    BOOST_CHECK(decoded.instance_id == original.instance_id);
    BOOST_CHECK(decoded.rack_id == original.rack_id);
    BOOST_CHECK(decoded.rebalance_timeout == original.rebalance_timeout);
    BOOST_TEST(decoded.member_epoch == original.member_epoch);
    BOOST_CHECK(
      decoded.subscribed_topic_names == original.subscribed_topic_names);
    BOOST_CHECK(
      decoded.subscribed_topic_regex == original.subscribed_topic_regex);
}

SEASTAR_THREAD_TEST_CASE(metadata_member_state_no_optionals) {
    consumer_group_member_state original;
    original.id = kafka::member_id("m-456");
    original.instance_id = std::nullopt;
    original.rack_id = std::nullopt;
    original.rebalance_timeout = std::chrono::milliseconds(30000);
    original.member_epoch = 0;
    original.subscribed_topic_names = {};
    original.subscribed_topic_regex = std::nullopt;

    iobuf buf;
    protocol::encoder writer(buf);
    consumer_group_member_state::encode(writer, original);

    auto reader = protocol::decoder(std::move(buf));
    auto decoded = consumer_group_member_state::decode(reader);

    BOOST_CHECK(decoded == original);
}

SEASTAR_THREAD_TEST_CASE(metadata_key_encode_decode_roundtrip) {
    consumer_group_metadata_key original;
    original.group_id = kafka::group_id("my-group");

    iobuf buf;
    protocol::encoder writer(buf);
    consumer_group_metadata_key::encode(writer, original);

    auto reader = protocol::decoder(std::move(buf));
    auto decoded = consumer_group_metadata_key::decode(reader);

    BOOST_TEST(decoded.group_id == original.group_id);
}

SEASTAR_THREAD_TEST_CASE(metadata_value_encode_decode_roundtrip) {
    consumer_group_metadata_value original;
    original.group_epoch = 42;
    original.target_assignment_epoch = 42;
    original.assignor = "uniform";
    original.state = static_cast<int8_t>(consumer_group_state::reconciling);
    original.state_timestamp = model::timestamp(1234567890);

    consumer_group_member_state ms1;
    ms1.id = kafka::member_id("m1");
    ms1.instance_id = std::nullopt;
    ms1.rack_id = std::nullopt;
    ms1.rebalance_timeout = std::chrono::milliseconds(30000);
    ms1.member_epoch = 42;
    ms1.subscribed_topic_names = {"t1", "t2"};
    ms1.subscribed_topic_regex = std::nullopt;

    consumer_group_member_state ms2;
    ms2.id = kafka::member_id("m2");
    ms2.instance_id = kafka::group_instance_id("static-2");
    ms2.rack_id = "eu-west-1";
    ms2.rebalance_timeout = std::chrono::milliseconds(60000);
    ms2.member_epoch = 41;
    ms2.subscribed_topic_names = {"t1"};
    ms2.subscribed_topic_regex = "t.*";

    original.members.push_back(std::move(ms1));
    original.members.push_back(std::move(ms2));

    iobuf buf;
    protocol::encoder writer(buf);
    consumer_group_metadata_value::encode(writer, original);

    auto reader = protocol::decoder(std::move(buf));
    auto decoded = consumer_group_metadata_value::decode(reader);

    BOOST_TEST(decoded.group_epoch == original.group_epoch);
    BOOST_TEST(
      decoded.target_assignment_epoch == original.target_assignment_epoch);
    BOOST_TEST(decoded.assignor == original.assignor);
    BOOST_TEST(decoded.state == original.state);
    BOOST_TEST(decoded.state_timestamp == original.state_timestamp);
    BOOST_REQUIRE(decoded.members.size() == 2);
    BOOST_TEST(decoded.members[0].id == kafka::member_id("m1"));
    BOOST_TEST(decoded.members[1].id == kafka::member_id("m2"));
    BOOST_CHECK(
      decoded.members[1].instance_id == kafka::group_instance_id("static-2"));
    BOOST_CHECK(decoded.members[1].rack_id == ss::sstring("eu-west-1"));
}

SEASTAR_THREAD_TEST_CASE(metadata_kv_serializer_roundtrip) {
    consumer_group_metadata_kv original;
    original.key.group_id = kafka::group_id("test-group");
    original.value = consumer_group_metadata_value{};
    original.value->group_epoch = 5;
    original.value->target_assignment_epoch = 5;
    original.value->assignor = "uniform";
    original.value->state = static_cast<int8_t>(consumer_group_state::stable);
    original.value->state_timestamp = model::timestamp(999);

    auto kv = group_metadata_serializer::to_kv(std::move(original));

    // Verify key type is consumer_group_metadata
    auto type = group_metadata_serializer::get_metadata_type(kv.key.copy());
    BOOST_TEST(type == group_metadata_type::consumer_group_metadata);
}

// --- Additional member convergence tests ---

SEASTAR_THREAD_TEST_CASE(member_convergence_empty_to_empty) {
    auto member = make_member("m1", {});
    // Both empty: is_at_target should be true
    BOOST_TEST(member->is_at_target());
}

SEASTAR_THREAD_TEST_CASE(member_convergence_partial_mismatch) {
    auto member = make_member("m1", {});

    auto tid1 = model::topic_id(uuid_t::create());
    auto tid2 = model::topic_id(uuid_t::create());

    // Target: tid1 -> {0,1}, tid2 -> {0}
    consumer_group_member::assignment_type target;
    target[tid1] = {model::partition_id(0), model::partition_id(1)};
    target[tid2] = {model::partition_id(0)};
    member->set_target_assignment(std::move(target));

    // Current: only tid1 -> {0,1} (missing tid2)
    consumer_group_member::assignment_type current;
    current[tid1] = {model::partition_id(0), model::partition_id(1)};
    member->set_current_assignment(std::move(current));

    BOOST_TEST(!member->is_at_target());
}

SEASTAR_THREAD_TEST_CASE(member_convergence_different_partitions) {
    auto member = make_member("m1", {});

    auto tid = model::topic_id(uuid_t::create());

    consumer_group_member::assignment_type target;
    target[tid] = {model::partition_id(0), model::partition_id(1)};
    member->set_target_assignment(std::move(target));

    // Current has different partition set
    consumer_group_member::assignment_type current;
    current[tid] = {model::partition_id(0), model::partition_id(2)};
    member->set_current_assignment(std::move(current));

    BOOST_TEST(!member->is_at_target());
}

SEASTAR_THREAD_TEST_CASE(member_should_keep_alive) {
    auto member = make_member("m1", {});
    auto now = ss::lowres_clock::now();
    member->set_latest_heartbeat(now);

    // Within session timeout: should be alive
    BOOST_TEST(member->should_keep_alive(
      now + std::chrono::seconds(1), std::chrono::seconds(10)));

    // Past session timeout: should not be alive
    BOOST_TEST(!member->should_keep_alive(
      now + std::chrono::seconds(11), std::chrono::seconds(10)));

    // Exactly at boundary: should not be alive (> not >=)
    BOOST_TEST(!member->should_keep_alive(
      now + std::chrono::seconds(10), std::chrono::seconds(10)));
}

// --- Assignor determinism tests ---

SEASTAR_THREAD_TEST_CASE(assignor_deterministic_output) {
    uniform_assignor assignor;

    auto tid = model::topic_id(uuid_t::create());
    std::vector<assignable_topic> topics;
    topics.push_back(
      assignable_topic{.id = tid, .name = "t1", .partition_count = 6});

    absl::node_hash_map<kafka::member_id, consumer_group_member_ptr> members;
    for (int i = 1; i <= 3; ++i) {
        auto id = fmt::format("m{}", i);
        chunked_vector<ss::sstring> sub;
        sub.push_back("t1");
        members[kafka::member_id(id)] = make_member(id, std::move(sub));
    }

    // Run assignor twice with same input
    auto result1 = assignor.assign(topics, members);
    auto result2 = assignor.assign(topics, members);

    // Results should be identical
    BOOST_TEST(result1.size() == result2.size());
    for (const auto& [mid, assignment] : result1) {
        auto it = result2.find(mid);
        BOOST_REQUIRE(it != result2.end());
        for (const auto& [tid_key, parts] : assignment) {
            auto tp_it = it->second.find(tid_key);
            BOOST_REQUIRE(tp_it != it->second.end());
            BOOST_TEST(parts == tp_it->second);
        }
    }
}

SEASTAR_THREAD_TEST_CASE(assignor_all_partitions_assigned) {
    uniform_assignor assignor;

    auto tid = model::topic_id(uuid_t::create());
    std::vector<assignable_topic> topics;
    topics.push_back(
      assignable_topic{.id = tid, .name = "t1", .partition_count = 7});

    absl::node_hash_map<kafka::member_id, consumer_group_member_ptr> members;
    for (int i = 1; i <= 3; ++i) {
        auto id = fmt::format("m{}", i);
        chunked_vector<ss::sstring> sub;
        sub.push_back("t1");
        members[kafka::member_id(id)] = make_member(id, std::move(sub));
    }

    auto result = assignor.assign(topics, members);

    // Collect all assigned partition IDs
    std::set<int32_t> assigned;
    for (const auto& [_, assignment] : result) {
        auto tp_it = assignment.find(tid);
        if (tp_it != assignment.end()) {
            for (const auto& p : tp_it->second) {
                assigned.insert(p());
            }
        }
    }

    // Every partition 0-6 should be assigned exactly once
    BOOST_TEST(assigned.size() == 7);
    for (int32_t i = 0; i < 7; ++i) {
        BOOST_TEST(assigned.count(i) == 1);
    }
}

SEASTAR_THREAD_TEST_CASE(assignor_balance_difference_at_most_one) {
    uniform_assignor assignor;

    auto tid = model::topic_id(uuid_t::create());
    std::vector<assignable_topic> topics;
    topics.push_back(
      assignable_topic{.id = tid, .name = "t1", .partition_count = 10});

    absl::node_hash_map<kafka::member_id, consumer_group_member_ptr> members;
    for (int i = 1; i <= 3; ++i) {
        auto id = fmt::format("m{}", i);
        chunked_vector<ss::sstring> sub;
        sub.push_back("t1");
        members[kafka::member_id(id)] = make_member(id, std::move(sub));
    }

    auto result = assignor.assign(topics, members);

    size_t min_count = SIZE_MAX;
    size_t max_count = 0;
    for (const auto& [_, assignment] : result) {
        auto tp_it = assignment.find(tid);
        size_t count = (tp_it != assignment.end()) ? tp_it->second.size() : 0;
        min_count = std::min(min_count, count);
        max_count = std::max(max_count, count);
    }

    // Difference between max and min should be at most 1
    BOOST_TEST(max_count - min_count <= 1);
}

// --- consumer_group_metadata_type detection ---

SEASTAR_THREAD_TEST_CASE(metadata_type_detection_key_version_3) {
    // Encode a consumer_group_metadata_key and check type detection
    consumer_group_metadata_key key;
    key.group_id = kafka::group_id("test-cg");

    iobuf buf;
    protocol::encoder writer(buf);
    // Write the version as the key prefix (this is how
    // decode_metadata_type works)
    writer.write(consumer_group_metadata_key::version());

    auto reader = protocol::decoder(buf.copy());
    auto type = decode_metadata_type(reader);
    BOOST_TEST(type == group_metadata_type::consumer_group_metadata);
}

SEASTAR_THREAD_TEST_CASE(metadata_type_detection_key_version_2) {
    // group_metadata_key has version 2
    iobuf buf;
    protocol::encoder writer(buf);
    writer.write(group_metadata_key::version());

    auto reader = protocol::decoder(buf.copy());
    auto type = decode_metadata_type(reader);
    BOOST_TEST(type == group_metadata_type::group_metadata);
}

SEASTAR_THREAD_TEST_CASE(metadata_type_detection_key_version_1) {
    // offset_metadata_key has version 1 (or 0)
    iobuf buf;
    protocol::encoder writer(buf);
    writer.write(offset_metadata_key::version());

    auto reader = protocol::decoder(buf.copy());
    auto type = decode_metadata_type(reader);
    BOOST_TEST(type == group_metadata_type::offset_commit);
}

// --- Full to_kv → to_record → decode round-trip ---

namespace {
model::record to_record_from_kv(group_metadata_serializer::key_value kv) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));
    builder.add_raw_kv(std::move(kv.key), std::move(kv.value));
    auto records = std::move(builder).build().copy_records();
    return std::move(records.front());
}
} // namespace

SEASTAR_THREAD_TEST_CASE(metadata_kv_full_decode_roundtrip) {
    consumer_group_metadata_kv original;
    original.key.group_id = kafka::group_id("roundtrip-group");
    original.value = consumer_group_metadata_value{};
    original.value->group_epoch = 10;
    original.value->target_assignment_epoch = 10;
    original.value->assignor = "uniform";
    original.value->state = static_cast<int8_t>(consumer_group_state::stable);
    original.value->state_timestamp = model::timestamp(555);

    consumer_group_member_state ms;
    ms.id = kafka::member_id("m1");
    ms.instance_id = kafka::group_instance_id("static-1");
    ms.rack_id = "us-west-2";
    ms.rebalance_timeout = std::chrono::milliseconds(45000);
    ms.member_epoch = 10;
    ms.subscribed_topic_names = {"t1", "t2"};
    ms.subscribed_topic_regex = std::nullopt;
    original.value->members.push_back(std::move(ms));

    auto kv = group_metadata_serializer::to_kv(std::move(original));
    auto decoded = group_metadata_serializer::decode_consumer_group_metadata(
      to_record_from_kv(std::move(kv)));

    BOOST_TEST(decoded.key.group_id == kafka::group_id("roundtrip-group"));
    BOOST_REQUIRE(decoded.value.has_value());
    BOOST_TEST(decoded.value->group_epoch == 10);
    BOOST_TEST(decoded.value->assignor == "uniform");
    BOOST_REQUIRE(decoded.value->members.size() == 1);
    BOOST_TEST(decoded.value->members[0].id == kafka::member_id("m1"));
    BOOST_CHECK(
      decoded.value->members[0].instance_id
      == kafka::group_instance_id("static-1"));
    BOOST_CHECK(decoded.value->members[0].rack_id == ss::sstring("us-west-2"));
}

SEASTAR_THREAD_TEST_CASE(metadata_kv_tombstone_decode) {
    // A tombstone has a key but no value (indicates group deletion)
    consumer_group_metadata_kv original;
    original.key.group_id = kafka::group_id("deleted-group");
    original.value = std::nullopt;

    auto kv = group_metadata_serializer::to_kv(std::move(original));

    // Build a tombstone record (key only, no value)
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));
    builder.add_raw_kv(std::move(kv.key), std::nullopt);
    auto records = std::move(builder).build().copy_records();

    auto decoded = group_metadata_serializer::decode_consumer_group_metadata(
      std::move(records.front()));

    BOOST_TEST(decoded.key.group_id == kafka::group_id("deleted-group"));
    BOOST_TEST(!decoded.value.has_value());
}

SEASTAR_THREAD_TEST_CASE(metadata_value_empty_members_roundtrip) {
    consumer_group_metadata_value original;
    original.group_epoch = 0;
    original.target_assignment_epoch = 0;
    original.assignor = "uniform";
    original.state = static_cast<int8_t>(consumer_group_state::empty);
    original.state_timestamp = model::timestamp(100);

    iobuf buf;
    protocol::encoder writer(buf);
    consumer_group_metadata_value::encode(writer, original);

    auto reader = protocol::decoder(std::move(buf));
    auto decoded = consumer_group_metadata_value::decode(reader);

    BOOST_CHECK(decoded == original);
    BOOST_TEST(decoded.members.empty());
}

SEASTAR_THREAD_TEST_CASE(metadata_value_preserves_all_states) {
    for (auto state :
         {consumer_group_state::empty,
          consumer_group_state::assigning,
          consumer_group_state::reconciling,
          consumer_group_state::stable,
          consumer_group_state::dead}) {
        consumer_group_metadata_value original;
        original.group_epoch = 1;
        original.target_assignment_epoch = 1;
        original.assignor = "uniform";
        original.state = static_cast<int8_t>(state);
        original.state_timestamp = model::timestamp::now();

        iobuf buf;
        protocol::encoder writer(buf);
        consumer_group_metadata_value::encode(writer, original);

        auto reader = protocol::decoder(std::move(buf));
        auto decoded = consumer_group_metadata_value::decode(reader);

        BOOST_TEST(decoded.state == static_cast<int8_t>(state));
    }
}

// --- consumer_group_probe tests ---

SEASTAR_THREAD_TEST_CASE(probe_group_counters) {
    consumer_group_probe probe;
    BOOST_TEST(probe.groups_total() == 0);

    probe.group_created();
    probe.group_created();
    BOOST_TEST(probe.groups_total() == 2);

    probe.group_deleted();
    BOOST_TEST(probe.groups_total() == 1);
}

SEASTAR_THREAD_TEST_CASE(probe_member_counters) {
    consumer_group_probe probe;
    BOOST_TEST(probe.members_total() == 0);

    probe.member_joined();
    probe.member_joined();
    probe.member_joined();
    BOOST_TEST(probe.members_total() == 3);

    probe.member_left();
    BOOST_TEST(probe.members_total() == 2);

    // Should not go below zero
    probe.member_left();
    probe.member_left();
    probe.member_left(); // extra call
    BOOST_TEST(probe.members_total() == 0);
}

SEASTAR_THREAD_TEST_CASE(probe_rebalance_and_heartbeat) {
    consumer_group_probe probe;
    BOOST_TEST(probe.rebalances_total() == 0);
    BOOST_TEST(probe.heartbeats_total() == 0);

    probe.rebalance_triggered();
    probe.rebalance_triggered();
    BOOST_TEST(probe.rebalances_total() == 2);

    probe.heartbeat_received();
    probe.heartbeat_received();
    probe.heartbeat_received();
    BOOST_TEST(probe.heartbeats_total() == 3);
}

} // namespace kafka
