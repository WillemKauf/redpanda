// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/controller_api.h"
#include "container/chunked_vector.h"
#include "kafka/protocol/find_coordinator.h"
#include "kafka/protocol/offset_commit.h"
#include "kafka/server/group_router.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/async.h"
#include "test_utils/scoped_config.h"

#include <seastar/coroutine/as_future.hh>
#include <seastar/testing/perf_tests.hh>

using namespace std::chrono_literals; // NOLINT

namespace {
ss::logger gclog("group_commit_bench");
} // namespace

/// Full-path offset commit benchmark: drives group_router::offset_commit on
/// a single-node fixture, covering the router hop, the group coordinator,
/// record serialization and raft replication of the __consumer_offsets
/// partition (single replica, so quorum ack is a local flush).
///
/// The burst cases fire many concurrent commits from distinct groups that
/// all map to one coordinator partition - the production regime in which a
/// coordinator shard saturates - and measure aggregate completion.
struct group_commit_fixture : redpanda_thread_fixture {
    static constexpr size_t n_groups = 64;

    model::topic topic{"bench-topic"};
    std::vector<kafka::group_id> groups;
    scoped_config cfg;
    int64_t committed_offset{0};

    group_commit_fixture() {
        cfg.get("group_topic_partitions").set_value(1);
        wait_for_controller_leadership().get();
        add_topic(model::topic_namespace_view(model::kafka_namespace, topic))
          .get();
        wait_for_consumer_offsets_topic();
        groups.reserve(n_groups);
        for (size_t i = 0; i < n_groups; ++i) {
            groups.emplace_back(fmt::format("bench-group-{}", i));
        }
        // warm up: the first commit of each group creates the group
        commit_burst(n_groups).get();
    }

    void wait_for_consumer_offsets_topic() {
        auto client = make_kafka_client().get();
        client.connect().get();
        kafka::find_coordinator_request req(kafka::group_id("bench-group-0"));
        req.data.key_type = kafka::coordinator_type::group;
        client.dispatch(std::move(req), kafka::api_version(1)).get();
        app.controller->get_api()
          .local()
          .wait_for_topic(
            model::kafka_consumer_offsets_nt, model::timeout_clock::now() + 30s)
          .get();
        client.stop().get();
        client.shutdown();

        tests::cooperative_spin_wait_with_timeout(30s, [this] {
            kafka::offset_commit_request req = make_request(0);
            auto stages = app.group_router.local().offset_commit(
              std::move(req));
            return std::move(stages.dispatched)
              .then([r = std::move(stages.result)]() mutable {
                  return std::move(r);
              })
              .then([](kafka::error_code ec) {
                  return ec == kafka::error_code::none;
              });
        }).get();
    }

    kafka::offset_commit_request make_request(size_t group_idx) {
        kafka::offset_commit_request req;
        req.data.group_id = groups.empty() ? kafka::group_id("bench-group-0")
                                           : groups[group_idx];
        req.data.topics.reserve(1);
        kafka::offset_commit_request_topic t;
        t.name = topic;
        t.partitions.push_back(
          kafka::offset_commit_request_partition{
            .partition_index = model::partition_id(0),
            .committed_offset = model::offset(++committed_offset),
          });
        req.data.topics.push_back(std::move(t));
        return req;
    }

    ss::future<> commit_burst(size_t concurrency) {
        std::vector<ss::future<kafka::error_code>> results;
        results.reserve(concurrency);
        for (size_t i = 0; i < concurrency; ++i) {
            auto stages = app.group_router.local().offset_commit(
              make_request(i % n_groups));
            co_await std::move(stages.dispatched);
            results.push_back(std::move(stages.result));
        }
        for (auto& f : results) {
            auto ec = co_await std::move(f);
            vassert(ec == kafka::error_code::none, "commit failed: {}", ec);
        }
    }

    ss::future<size_t> run_burst(size_t concurrency) {
        co_await tests::drain_task_queue();
        perf_tests::start_measuring_time();
        co_await commit_burst(concurrency);
        perf_tests::stop_measuring_time();
        co_return concurrency;
    }
};

PERF_TEST_CN(group_commit_fixture, commit_serial) {
    co_return co_await this->run_burst(1);
}

PERF_TEST_CN(group_commit_fixture, commit_burst_8) {
    co_return co_await this->run_burst(8);
}

PERF_TEST_CN(group_commit_fixture, commit_burst_64) {
    co_return co_await this->run_burst(64);
}
