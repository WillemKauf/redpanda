/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/meta.h"
#include "cloud_topics/level_one/compaction/worker.h"
#include "cloud_topics/level_one/compaction/worker_manager.h"
#include "model/fundamental.h"
#include "test_utils/test.h"

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

#include <chrono>

using namespace cloud_topics;
using namespace std::chrono_literals;

class WorkerManagerTestFixture : public seastar_test {
public:
    ss::future<> start_workers(l1::worker_manager& manager) {
        co_await manager._workers.start(&manager, nullptr, nullptr, nullptr);
        co_await manager._workers.invoke_on_all(&l1::compaction_worker::start);
    }

    ss::future<l1::compaction_worker::worker_state>
    get_worker_state(l1::worker_manager& manager, ss::shard_id shard) {
        return manager._workers.invoke_on(
          shard,
          [](l1::compaction_worker& worker) { return worker._worker_state; });
    }

    ss::future<bool>
    worker_bg_fut_has_value(l1::worker_manager& manager, ss::shard_id shard) {
        return manager._workers.invoke_on(
          shard, [](l1::compaction_worker& worker) {
              return worker._bg_fut.has_value();
          });
    }
};

TEST_F(WorkerManagerTestFixture, PauseAndResumeWorkers) {
    l1::pq_t pq;
    l1::worker_manager manager(pq, nullptr, nullptr, nullptr);
    start_workers(manager).get();
    auto stop_manager = ss::defer([&manager] { manager.stop().get(); });
    using worker_state = l1::compaction_worker::worker_state;
    for (ss::shard_id i = 0; i < ss::smp::count; ++i) {
        // Workers start in active state
        ASSERT_EQ(get_worker_state(manager, i).get(), worker_state::active);
        ASSERT_TRUE(worker_bg_fut_has_value(manager, i).get());

        // Pause workers and expect to see state reflect that.
        manager.pause_worker(i).get();
        ASSERT_EQ(get_worker_state(manager, i).get(), worker_state::paused);
        ASSERT_FALSE(worker_bg_fut_has_value(manager, i).get());

        // Resume workers and expect to see active state.
        manager.resume_worker(i).get();
        ASSERT_EQ(get_worker_state(manager, i).get(), worker_state::active);
        ASSERT_TRUE(worker_bg_fut_has_value(manager, i).get());
    }
}

TEST_F(WorkerManagerTestFixture, AcquireAndFinishWork) {
    auto cmp_func =
      [](const l1::log_compaction_meta* a, const l1::log_compaction_meta* b) {
          return a->ntp < b->ntp;
      };
    l1::pq_t pq(std::move(cmp_func));
    l1::log_list_t list;
    l1::worker_manager manager(pq, nullptr, nullptr, nullptr);
    auto stop_manager = ss::defer([&manager] { manager.stop().get(); });

    const auto test_ntp = model::ntp(
      model::ns("kafka"), model::topic("tapioca"), model::partition_id(0));
    const auto test_tidp = model::topic_id_partition(
      model::topic_id(uuid_t::create()), test_ntp.tp.partition);
    auto meta = std::make_unique<l1::log_compaction_meta>(test_tidp, test_ntp);
    list.push_back(*meta);
    pq.emplace(meta.get());

    ASSERT_EQ(meta->inflight, false);

    auto shard = ss::this_shard_id();
    auto work_opt = manager.try_acquire_work(shard);
    ASSERT_TRUE(work_opt.has_value());
    ASSERT_EQ(work_opt.value()->ntp, test_ntp);
    ASSERT_EQ(work_opt.value()->tidp, test_tidp);
    ASSERT_EQ(meta->inflight, true);
    manager.finish_work(work_opt.value(), shard);
    ASSERT_EQ(meta->inflight, false);
}
