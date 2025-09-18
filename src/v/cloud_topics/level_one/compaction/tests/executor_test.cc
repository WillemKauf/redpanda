/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/executor.h"
#include "model/fundamental.h"
#include "test_utils/test.h"

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

#include <chrono>

using namespace cloud_topics;
using namespace std::chrono_literals;

class ExecutorTestFixture : public seastar_test {
public:
    using worker_shard = l1::compaction_executor::worker_shard;
    using worker_state = l1::compaction_executor::worker_meta::state;

    ss::future<> fake_start_workers(l1::compaction_executor& executor) {
        co_await executor._workers.start(nullptr, nullptr, nullptr);
    }

    void populate_workers(l1::compaction_executor& executor) {
        executor.populate_workers();
    }

    const l1::compaction_executor::worker_set&
    get_worker_meta(const l1::compaction_executor& executor) {
        return executor._worker_meta;
    }

    const l1::compaction_executor::worker_list&
    get_available_workers(const l1::compaction_executor& executor) {
        return executor._avail_workers;
    }

    ss::future<worker_shard> get_available_worker(
      l1::compaction_executor& executor,
      const model::topic_id_partition& tidp) {
        co_return co_await executor.get_available_worker(tidp);
    }

    void return_worker(
      l1::compaction_executor& executor,
      worker_shard worker,
      model::topic_id_partition tidp) {
        executor.return_worker(worker, std::move(tidp));
    }
};

TEST_F(ExecutorTestFixture, PopulateWorkers) {
    l1::compaction_executor executor(nullptr, nullptr, nullptr);
    populate_workers(executor);

    auto shard_count = ss::smp::count;
    auto& worker_meta = get_worker_meta(executor);
    auto& avail_workers = get_available_workers(executor);

    ASSERT_EQ(worker_meta.size(), shard_count);
    ASSERT_EQ(avail_workers.size(), shard_count);
}

TEST_F(ExecutorTestFixture, PauseAndUnpauseWorkerState) {
    l1::compaction_executor executor(nullptr, nullptr, nullptr);
    populate_workers(executor);

    auto shard_count = ss::smp::count;
    auto& worker_meta = get_worker_meta(executor);
    auto& avail_workers = get_available_workers(executor);

    auto worker = worker_shard{0};

    // Get the worker metadata
    auto it = worker_meta.find(worker);
    ASSERT_TRUE(it != worker_meta.end());
    auto& meta = *it->get();
    ASSERT_EQ(meta.shard, worker);

    // Pausing a worker should remove it from the pool of available workers.
    executor.set_worker_state(worker, worker_state::paused).get();
    ASSERT_EQ(meta.current_state, worker_state::paused);
    ASSERT_EQ(worker_meta.size(), shard_count);
    ASSERT_EQ(avail_workers.size(), shard_count - 1);
    ASSERT_EQ(avail_workers.front().shard, worker_shard{1});

    // Setting it back to active pushes it back to the of the list of available
    // workers.
    executor.set_worker_state(worker, worker_state::active).get();
    ASSERT_EQ(meta.current_state, worker_state::active);
    ASSERT_EQ(worker_meta.size(), shard_count);
    ASSERT_EQ(avail_workers.size(), shard_count);
    ASSERT_EQ(avail_workers.back().shard, worker);

    // Doing it again is be idempotent (the worker should not be present
    // multiple times in the pool of available workers).
    executor.set_worker_state(worker, worker_state::active).get();
    ASSERT_EQ(meta.current_state, worker_state::active);
    ASSERT_EQ(worker_meta.size(), shard_count);
    ASSERT_EQ(avail_workers.size(), shard_count);
    ASSERT_EQ(avail_workers.back().shard, worker);
}

TEST_F(ExecutorTestFixture, PauseInflightWorker) {
    const auto tidp = model::topic_id_partition(
      model::topic_id(uuid_t::create()), model::partition_id(0));
    l1::compaction_executor executor(nullptr, nullptr, nullptr);
    populate_workers(executor);
    fake_start_workers(executor).get();
    auto stop_executor = ss::defer([&executor] { executor.stop().get(); });

    auto shard_count = ss::smp::count;
    const auto& worker_meta = get_worker_meta(executor);
    const auto& avail_workers = get_available_workers(executor);

    // Get the first available worker from the pool
    auto worker = get_available_worker(executor, tidp).get();
    ASSERT_EQ(worker, worker_shard{0});
    ASSERT_EQ(worker_meta.size(), shard_count);
    ASSERT_EQ(avail_workers.size(), shard_count - 1);

    // Get the worker metadata
    auto it = worker_meta.find(worker);
    ASSERT_TRUE(it != worker_meta.end());
    auto& meta = *it->get();
    ASSERT_EQ(meta.shard, worker_shard{0});

    // Check that the worker metadata has been updated to reflect the inflight
    // compaction.
    ASSERT_TRUE(meta.tidp.has_value());
    ASSERT_EQ(meta.tidp.value(), tidp);
    ASSERT_EQ(meta.current_state, worker_state::active);

    // Set the inflight worker's state to paused.
    executor.set_worker_state(worker_shard{0}, worker_state::paused).get();
    ASSERT_EQ(meta.current_state, worker_state::paused);

    // Return the worker, and see that it has cleared its metadata.
    return_worker(executor, worker, tidp);
    ASSERT_FALSE(meta.tidp.has_value());

    // The worker is paused, so it should _not_ be returned to the worker pool.
    ASSERT_EQ(worker, worker_shard{0});
    ASSERT_EQ(worker_meta.size(), shard_count);
    ASSERT_EQ(avail_workers.size(), shard_count - 1);

    // Setting it back to active pushes it back to the of the list of available
    // workers.
    executor.set_worker_state(worker_shard{0}, worker_state::active).get();
    ASSERT_EQ(worker_meta.size(), shard_count);
    ASSERT_EQ(avail_workers.size(), shard_count);
    ASSERT_EQ(avail_workers.back().shard, worker_shard{0});
}

TEST_F(ExecutorTestFixture, PauseAndUnpauseInflightWorker) {
    const auto tidp = model::topic_id_partition(
      model::topic_id(uuid_t::create()), model::partition_id(0));
    l1::compaction_executor executor(nullptr, nullptr, nullptr);
    populate_workers(executor);
    fake_start_workers(executor).get();
    auto stop_executor = ss::defer([&executor] { executor.stop().get(); });

    auto shard_count = ss::smp::count;
    const auto& worker_meta = get_worker_meta(executor);
    const auto& avail_workers = get_available_workers(executor);

    // Get the first available worker from the pool
    auto worker = get_available_worker(executor, tidp).get();
    ASSERT_EQ(worker, worker_shard{0});
    ASSERT_EQ(worker_meta.size(), shard_count);
    ASSERT_EQ(avail_workers.size(), shard_count - 1);

    // Get the worker metadata
    auto it = worker_meta.find(worker);
    ASSERT_TRUE(it != worker_meta.end());
    auto& meta = *it->get();
    ASSERT_EQ(meta.shard, worker_shard{0});

    // Check that the worker metadata has been updated to reflect the inflight
    // compaction.
    ASSERT_TRUE(meta.tidp.has_value());
    ASSERT_EQ(meta.tidp.value(), tidp);
    ASSERT_EQ(meta.current_state, worker_state::active);

    // Set the inflight worker's state to paused.
    executor.set_worker_state(worker_shard{0}, worker_state::paused).get();
    ASSERT_EQ(meta.current_state, worker_state::paused);

    // Set it back to active.
    executor.set_worker_state(worker_shard{0}, worker_state::active).get();
    ASSERT_EQ(meta.current_state, worker_state::active);

    // Return the worker, and see that it has cleared its metadata.
    return_worker(executor, worker, tidp);
    ASSERT_FALSE(meta.tidp.has_value());

    // The worker is active, so it should be returned to the worker pool (but
    // only once!)
    ASSERT_EQ(worker, worker_shard{0});
    ASSERT_EQ(worker_meta.size(), shard_count);
    ASSERT_EQ(avail_workers.size(), shard_count);
}

TEST_F(ExecutorTestFixture, WaitingOnEmptyPool) {
    const auto tidp = model::topic_id_partition(
      model::topic_id(uuid_t::create()), model::partition_id(0));
    l1::compaction_executor executor(nullptr, nullptr, nullptr);
    populate_workers(executor);
    fake_start_workers(executor).get();
    auto stop_executor = ss::defer([&executor] { executor.stop().get(); });

    auto shard_count = ss::smp::count;
    const auto& avail_workers = get_available_workers(executor);

    // Steal all the available workers from the pool.
    std::vector<worker_shard> workers;
    workers.reserve(shard_count);
    for (size_t i = 0; i < shard_count; ++i) {
        workers.push_back(get_available_worker(executor, tidp).get());
    }

    ASSERT_EQ(workers.size(), shard_count);
    ASSERT_TRUE(avail_workers.empty());

    // Future that eventually returns all workers to the pool.
    auto return_worker_fut = ss::do_until(
      [&] { return workers.empty(); },
      [&] {
          return ss::sleep(200ms).then([&] {
              auto worker = workers.back();
              workers.pop_back();
              return_worker(executor, worker, tidp);
              return ss::now();
          });
      });

    // Future that waits until all workers are succesfully returned
    // and re-acquired.
    size_t acquired_workers = 0;
    auto wait_for_worker_fut = ss::do_until(
      [&] { return acquired_workers == shard_count; },
      [&] {
          return get_available_worker(executor, tidp).then([&](worker_shard) {
              ++acquired_workers;
          });
      });

    // Returning a worker to an empty pool should trigger a condition variable
    // and wake a waiter.
    ss::when_all(std::move(return_worker_fut), std::move(wait_for_worker_fut))
      .get();
}
