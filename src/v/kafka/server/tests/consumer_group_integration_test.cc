// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/controller.h"
#include "cluster/controller_api.h"
#include "container/chunked_vector.h"
#include "kafka/client/transport.h"
#include "kafka/protocol/consumer_group_heartbeat.h"
#include "kafka/protocol/delete_groups.h"
#include "kafka/protocol/describe_groups.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/find_coordinator.h"
#include "kafka/protocol/join_group.h"
#include "kafka/protocol/list_groups.h"
#include "kafka/protocol/offset_commit.h"
#include "kafka/protocol/offset_fetch.h"
#include "kafka/server/coordinator_ntp_mapper.h"
#include "kafka/server/group_manager.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/async.h"
#include "test_utils/boost_fixture.h"
#include "test_utils/scoped_config.h"

#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>

#include <boost/test/tools/old/interface.hpp>

using namespace std::chrono_literals;
using namespace kafka;

namespace {

/// Send a ConsumerGroupHeartbeat join request (epoch=0).
consumer_group_heartbeat_response do_join(
  kafka::client::transport& client,
  const ss::sstring& group_id,
  const std::vector<ss::sstring>& topics) {
    consumer_group_heartbeat_request req;
    req.data.group_id = kafka::group_id(group_id);
    req.data.member_id = kafka::member_id("");
    req.data.member_epoch = kafka::consumer_group_member_epoch(0);
    chunked_vector<ss::sstring> cv;
    for (const auto& t : topics) {
        cv.push_back(t);
    }
    req.data.subscribed_topic_names = std::move(cv);
    return client.dispatch(std::move(req), kafka::api_version(0)).get();
}

/// Send a regular heartbeat (epoch > 0).
consumer_group_heartbeat_response do_heartbeat(
  kafka::client::transport& client,
  const ss::sstring& group_id,
  const ss::sstring& member_id,
  int32_t member_epoch) {
    consumer_group_heartbeat_request req;
    req.data.group_id = kafka::group_id(group_id);
    req.data.member_id = kafka::member_id(member_id);
    req.data.member_epoch = kafka::consumer_group_member_epoch(member_epoch);
    return client.dispatch(std::move(req), kafka::api_version(0)).get();
}

/// Send a leave heartbeat (epoch = -1).
consumer_group_heartbeat_response do_leave(
  kafka::client::transport& client,
  const ss::sstring& group_id,
  const ss::sstring& member_id) {
    consumer_group_heartbeat_request req;
    req.data.group_id = kafka::group_id(group_id);
    req.data.member_id = kafka::member_id(member_id);
    req.data.member_epoch = kafka::consumer_group_member_epoch(-1);
    return client.dispatch(std::move(req), kafka::api_version(0)).get();
}

} // namespace

/// Fixture that creates the __consumer_offsets topic.
struct kip848_fixture : public redpanda_thread_fixture {
    void wait_for_consumer_offsets_topic() {
        auto client = make_kafka_client().get();
        client.connect().get();

        // Trigger consumer offsets topic creation via find_coordinator
        kafka::find_coordinator_request req(
          kafka::group_instance_id("kip848-init"));
        req.data.key_type = kafka::coordinator_type::group;
        client.dispatch(std::move(req), kafka::api_version(1)).get();

        app.controller->get_api()
          .local()
          .wait_for_topic(
            model::kafka_consumer_offsets_nt, model::timeout_clock::now() + 30s)
          .get();

        // Wait until the coordinator is ready
        tests::cooperative_spin_wait_with_timeout(30s, [&client] {
            kafka::describe_groups_request dreq;
            dreq.data.groups.emplace_back(
              kafka::group_instance_id("kip848-init"));
            return client.dispatch(std::move(dreq), kafka::api_version(1))
              .then([](kafka::describe_groups_response response) {
                  return response.data.groups.front().error_code
                         == kafka::error_code::none;
              });
        }).get();

        client.stop().get();
        client.shutdown();
    }
};

// --- Group lifecycle tests ---

FIXTURE_TEST(join_consumer_group_test, kip848_fixture) {
    scoped_config cfg;
    cfg.get("group_topic_partitions").set_value(1);
    wait_for_consumer_offsets_topic();

    auto client = make_kafka_client().get();
    auto deferred = ss::defer([&client] {
        client.stop().then([&client] { client.shutdown(); }).get();
    });
    client.connect().get();

    std::vector<ss::sstring> topics = {"t1"};

    tests::cooperative_spin_wait_with_timeout(30s, [&client, &topics] {
        auto resp = do_join(client, "cg-join-test", topics);
        if (resp.data.error_code != kafka::error_code::none) {
            return false;
        }
        BOOST_REQUIRE(resp.data.member_id.has_value());
        BOOST_CHECK(!resp.data.member_id->operator()().empty());
        BOOST_CHECK(resp.data.member_epoch >= 0);
        BOOST_CHECK(resp.data.heartbeat_interval_ms > 0);
        return true;
    }).get();
}

FIXTURE_TEST(leave_consumer_group_test, kip848_fixture) {
    scoped_config cfg;
    cfg.get("group_topic_partitions").set_value(1);
    wait_for_consumer_offsets_topic();

    auto client = make_kafka_client().get();
    auto deferred = ss::defer([&client] {
        client.stop().then([&client] { client.shutdown(); }).get();
    });
    client.connect().get();

    std::vector<ss::sstring> topics = {"t1"};

    // Join first
    consumer_group_heartbeat_response join_resp;
    tests::cooperative_spin_wait_with_timeout(
      30s, [&client, &topics, &join_resp] {
          join_resp = do_join(client, "cg-leave-test", topics);
          return join_resp.data.error_code == kafka::error_code::none;
      })
      .get();

    BOOST_REQUIRE(join_resp.data.member_id.has_value());
    auto member_id = *join_resp.data.member_id;

    // Leave
    auto leave_resp = do_leave(client, "cg-leave-test", member_id);
    BOOST_CHECK(leave_resp.data.error_code == kafka::error_code::none);
    BOOST_CHECK(leave_resp.data.member_epoch == -1);
}

FIXTURE_TEST(heartbeat_keeps_alive_test, kip848_fixture) {
    scoped_config cfg;
    cfg.get("group_topic_partitions").set_value(1);
    wait_for_consumer_offsets_topic();

    auto client = make_kafka_client().get();
    auto deferred = ss::defer([&client] {
        client.stop().then([&client] { client.shutdown(); }).get();
    });
    client.connect().get();

    std::vector<ss::sstring> topics = {"t1"};

    // Join
    consumer_group_heartbeat_response join_resp;
    tests::cooperative_spin_wait_with_timeout(
      30s, [&client, &topics, &join_resp] {
          join_resp = do_join(client, "cg-hb-test", topics);
          return join_resp.data.error_code == kafka::error_code::none;
      })
      .get();

    auto member_id = *join_resp.data.member_id;
    auto epoch = join_resp.data.member_epoch;

    // Send regular heartbeat
    auto hb_resp = do_heartbeat(client, "cg-hb-test", member_id, epoch);
    BOOST_CHECK(hb_resp.data.error_code == kafka::error_code::none);
}

FIXTURE_TEST(heartbeat_fenced_epoch_test, kip848_fixture) {
    scoped_config cfg;
    cfg.get("group_topic_partitions").set_value(1);
    wait_for_consumer_offsets_topic();

    auto client = make_kafka_client().get();
    auto deferred = ss::defer([&client] {
        client.stop().then([&client] { client.shutdown(); }).get();
    });
    client.connect().get();

    std::vector<ss::sstring> topics = {"t1"};

    // Join
    consumer_group_heartbeat_response join_resp;
    tests::cooperative_spin_wait_with_timeout(
      30s, [&client, &topics, &join_resp] {
          join_resp = do_join(client, "cg-fenced-test", topics);
          return join_resp.data.error_code == kafka::error_code::none;
      })
      .get();

    auto member_id = *join_resp.data.member_id;

    // Send heartbeat with wrong epoch
    auto hb_resp = do_heartbeat(client, "cg-fenced-test", member_id, 9999);
    BOOST_CHECK(
      hb_resp.data.error_code == kafka::error_code::fenced_member_epoch);
}

FIXTURE_TEST(heartbeat_unknown_member_test, kip848_fixture) {
    scoped_config cfg;
    cfg.get("group_topic_partitions").set_value(1);
    wait_for_consumer_offsets_topic();

    auto client = make_kafka_client().get();
    auto deferred = ss::defer([&client] {
        client.stop().then([&client] { client.shutdown(); }).get();
    });
    client.connect().get();

    std::vector<ss::sstring> topics = {"t1"};

    // First create the group
    tests::cooperative_spin_wait_with_timeout(30s, [&client, &topics] {
        auto resp = do_join(client, "cg-unknown-member-test", topics);
        return resp.data.error_code == kafka::error_code::none;
    }).get();

    // Now send heartbeat with nonexistent member
    auto hb_resp = do_heartbeat(
      client, "cg-unknown-member-test", "fake-member-id", 1);
    BOOST_CHECK(
      hb_resp.data.error_code == kafka::error_code::unknown_member_id);
}

FIXTURE_TEST(join_max_group_size_test, kip848_fixture) {
    scoped_config cfg;
    cfg.get("group_topic_partitions").set_value(1);
    cfg.get("consumer_group_max_size").set_value(int32_t(1));
    wait_for_consumer_offsets_topic();

    auto client = make_kafka_client().get();
    auto deferred = ss::defer([&client] {
        client.stop().then([&client] { client.shutdown(); }).get();
    });
    client.connect().get();

    std::vector<ss::sstring> topics = {"t1"};

    // First join should succeed
    tests::cooperative_spin_wait_with_timeout(30s, [&client, &topics] {
        auto r = do_join(client, "cg-max-size-test", topics);
        return r.data.error_code == kafka::error_code::none;
    }).get();

    // Second join should fail with group_max_size_reached
    auto resp2 = do_join(client, "cg-max-size-test", topics);
    BOOST_CHECK(
      resp2.data.error_code == kafka::error_code::group_max_size_reached);
}

// --- Protocol isolation tests ---

FIXTURE_TEST(classic_rejected_for_consumer_group_test, kip848_fixture) {
    scoped_config cfg;
    cfg.get("group_topic_partitions").set_value(1);
    wait_for_consumer_offsets_topic();

    auto client = make_kafka_client().get();
    auto deferred = ss::defer([&client] {
        client.stop().then([&client] { client.shutdown(); }).get();
    });
    client.connect().get();

    std::vector<ss::sstring> topics = {"t1"};

    // Create KIP-848 group
    tests::cooperative_spin_wait_with_timeout(30s, [&client, &topics] {
        auto r = do_join(client, "cg-isolation-test", topics);
        return r.data.error_code == kafka::error_code::none;
    }).get();

    // Try classic JoinGroup for same group_id
    join_group_request jg_req;
    jg_req.data.group_id = kafka::group_id("cg-isolation-test");
    jg_req.data.member_id = kafka::member_id("");
    jg_req.data.protocol_type = kafka::protocol_type("consumer");
    jg_req.data.protocols.push_back(join_group_request_protocol{
      .name = protocol_name("range"), .metadata = bytes{}});
    jg_req.data.session_timeout_ms = 10s;

    auto jg_resp
      = client.dispatch(std::move(jg_req), kafka::api_version(5)).get();
    BOOST_CHECK(
      jg_resp.data.error_code == kafka::error_code::group_id_not_found);
}

FIXTURE_TEST(
  consumer_heartbeat_rejected_for_classic_group_test, kip848_fixture) {
    scoped_config cfg;
    cfg.get("group_topic_partitions").set_value(1);
    wait_for_consumer_offsets_topic();

    auto client = make_kafka_client().get();
    auto deferred = ss::defer([&client] {
        client.stop().then([&client] { client.shutdown(); }).get();
    });
    client.connect().get();

    // Create classic group via JoinGroup
    tests::cooperative_spin_wait_with_timeout(30s, [&client] {
        join_group_request jg_req;
        jg_req.data.group_id = kafka::group_id("classic-isolation-test");
        jg_req.data.member_id = kafka::member_id("");
        jg_req.data.protocol_type = kafka::protocol_type("consumer");
        jg_req.data.protocols.push_back(join_group_request_protocol{
          .name = protocol_name("range"), .metadata = bytes{}});
        jg_req.data.session_timeout_ms = 30s;
        auto resp
          = client.dispatch(std::move(jg_req), kafka::api_version(5)).get();
        return resp.data.error_code == kafka::error_code::none
               || resp.data.error_code
                    == kafka::error_code::member_id_required;
    }).get();

    // Try ConsumerGroupHeartbeat for same group_id
    std::vector<ss::sstring> topics = {"t1"};
    auto resp = do_join(client, "classic-isolation-test", topics);
    BOOST_CHECK(
      resp.data.error_code == kafka::error_code::group_id_not_found);
}

// --- Offset commit/fetch tests ---

FIXTURE_TEST(offset_commit_and_fetch_test, kip848_fixture) {
    scoped_config cfg;
    cfg.get("group_topic_partitions").set_value(1);
    add_topic(
      model::topic_namespace_view{model::kafka_namespace, model::topic{"foo"}},
      3)
      .get();
    wait_for_consumer_offsets_topic();

    auto client = make_kafka_client().get();
    auto deferred = ss::defer([&client] {
        client.stop().then([&client] { client.shutdown(); }).get();
    });
    client.connect().get();

    std::vector<ss::sstring> topics = {"foo"};

    // Join KIP-848 group
    consumer_group_heartbeat_response join_resp;
    tests::cooperative_spin_wait_with_timeout(
      30s, [&client, &topics, &join_resp] {
          join_resp = do_join(client, "cg-offset-test", topics);
          return join_resp.data.error_code == kafka::error_code::none;
      })
      .get();

    auto member_id_str = *join_resp.data.member_id;
    auto epoch = join_resp.data.member_epoch;

    // Commit offsets
    offset_commit_request oc_req;
    oc_req.data.group_id = kafka::group_id("cg-offset-test");
    oc_req.data.member_id = member_id_str;
    oc_req.data.generation_id = epoch;
    oc_req.data.topics.push_back(offset_commit_request_topic{
      .name = model::topic{"foo"},
      .partitions = {offset_commit_request_partition{
        .partition_index = model::partition_id{0},
        .committed_offset = model::offset{42}}}});

    auto oc_resp
      = client.dispatch(std::move(oc_req), kafka::api_version(7)).get();
    BOOST_REQUIRE(!oc_resp.data.errored());

    // Fetch offsets
    offset_fetch_request of_req;
    offset_fetch_request_group of_group;
    of_group.group_id = kafka::group_id("cg-offset-test");
    chunked_vector<offset_fetch_request_topics> of_topics;
    offset_fetch_request_topics of_topic;
    of_topic.name = model::topic{"foo"};
    of_topic.partition_indexes.push_back(model::partition_id{0});
    of_topics.push_back(std::move(of_topic));
    of_group.topics = std::move(of_topics);
    of_req.data.groups.push_back(std::move(of_group));

    auto of_resp
      = client.dispatch(std::move(of_req), kafka::api_version(8)).get();
    BOOST_REQUIRE(!of_resp.data.groups.empty());
    auto& g = of_resp.data.groups.front();
    BOOST_CHECK(g.error_code == kafka::error_code::none);
    BOOST_REQUIRE(!g.topics.empty());
    BOOST_REQUIRE(!g.topics.front().partitions.empty());
    BOOST_CHECK(
      g.topics.front().partitions.front().committed_offset
      == model::offset{42});
}

// --- ListGroups / DescribeGroups / DeleteGroups ---

FIXTURE_TEST(list_groups_includes_consumer_groups_test, kip848_fixture) {
    scoped_config cfg;
    cfg.get("group_topic_partitions").set_value(1);
    wait_for_consumer_offsets_topic();

    auto client = make_kafka_client().get();
    auto deferred = ss::defer([&client] {
        client.stop().then([&client] { client.shutdown(); }).get();
    });
    client.connect().get();

    std::vector<ss::sstring> topics = {"t1"};

    // Create KIP-848 group
    tests::cooperative_spin_wait_with_timeout(30s, [&client, &topics] {
        auto r = do_join(client, "cg-list-test", topics);
        return r.data.error_code == kafka::error_code::none;
    }).get();

    // List groups
    list_groups_request lg_req;
    auto lg_resp
      = client.dispatch(std::move(lg_req), kafka::api_version(0)).get();
    BOOST_CHECK(lg_resp.data.error_code == kafka::error_code::none);

    // Find our group in the list
    bool found = false;
    for (const auto& g : lg_resp.data.groups) {
        if (g.group_id == kafka::group_id("cg-list-test")) {
            found = true;
            BOOST_CHECK(
              g.protocol_type == kafka::protocol_type("consumer"));
            break;
        }
    }
    BOOST_CHECK(found);
}

FIXTURE_TEST(describe_consumer_group_test, kip848_fixture) {
    scoped_config cfg;
    cfg.get("group_topic_partitions").set_value(1);
    wait_for_consumer_offsets_topic();

    auto client = make_kafka_client().get();
    auto deferred = ss::defer([&client] {
        client.stop().then([&client] { client.shutdown(); }).get();
    });
    client.connect().get();

    std::vector<ss::sstring> topics = {"t1"};

    // Create KIP-848 group
    tests::cooperative_spin_wait_with_timeout(30s, [&client, &topics] {
        auto r = do_join(client, "cg-describe-test", topics);
        return r.data.error_code == kafka::error_code::none;
    }).get();

    // Describe group
    describe_groups_request dg_req;
    dg_req.data.groups.emplace_back(kafka::group_id("cg-describe-test"));
    auto dg_resp
      = client.dispatch(std::move(dg_req), kafka::api_version(1)).get();

    BOOST_REQUIRE(!dg_resp.data.groups.empty());
    auto& g = dg_resp.data.groups.front();
    BOOST_CHECK(g.error_code == kafka::error_code::none);
    BOOST_CHECK(g.protocol_type == kafka::protocol_type("consumer"));
}

FIXTURE_TEST(delete_empty_consumer_group_test, kip848_fixture) {
    scoped_config cfg;
    cfg.get("group_topic_partitions").set_value(1);
    wait_for_consumer_offsets_topic();

    auto client = make_kafka_client().get();
    auto deferred = ss::defer([&client] {
        client.stop().then([&client] { client.shutdown(); }).get();
    });
    client.connect().get();

    std::vector<ss::sstring> topics = {"t1"};

    // Create KIP-848 group then leave
    consumer_group_heartbeat_response join_resp;
    tests::cooperative_spin_wait_with_timeout(
      30s, [&client, &topics, &join_resp] {
          join_resp = do_join(client, "cg-delete-test", topics);
          return join_resp.data.error_code == kafka::error_code::none;
      })
      .get();

    auto member_id = *join_resp.data.member_id;
    auto leave_resp = do_leave(client, "cg-delete-test", member_id);
    BOOST_REQUIRE(leave_resp.data.error_code == kafka::error_code::none);

    // Delete the now-empty group
    delete_groups_request del_req;
    del_req.data.groups_names.push_back(kafka::group_id("cg-delete-test"));
    auto del_resp
      = client.dispatch(std::move(del_req), kafka::api_version(0)).get();

    BOOST_REQUIRE(!del_resp.data.results.empty());
    BOOST_CHECK(
      del_resp.data.results.front().error_code == kafka::error_code::none);
}

FIXTURE_TEST(delete_non_empty_consumer_group_fails_test, kip848_fixture) {
    scoped_config cfg;
    cfg.get("group_topic_partitions").set_value(1);
    wait_for_consumer_offsets_topic();

    auto client = make_kafka_client().get();
    auto deferred = ss::defer([&client] {
        client.stop().then([&client] { client.shutdown(); }).get();
    });
    client.connect().get();

    std::vector<ss::sstring> topics = {"t1"};

    // Create KIP-848 group (don't leave)
    tests::cooperative_spin_wait_with_timeout(30s, [&client, &topics] {
        auto r = do_join(client, "cg-delete-nonempty-test", topics);
        return r.data.error_code == kafka::error_code::none;
    }).get();

    // Try to delete non-empty group
    delete_groups_request del_req;
    del_req.data.groups_names.push_back(
      kafka::group_id("cg-delete-nonempty-test"));
    auto del_resp
      = client.dispatch(std::move(del_req), kafka::api_version(0)).get();

    BOOST_REQUIRE(!del_resp.data.results.empty());
    BOOST_CHECK(
      del_resp.data.results.front().error_code
      == kafka::error_code::non_empty_group);
}
