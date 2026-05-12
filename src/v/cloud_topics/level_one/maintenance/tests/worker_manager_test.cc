/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/maintenance/meta.h"
#include "cloud_topics/level_one/maintenance/scheduler_probe.h"
#include "cloud_topics/level_one/maintenance/worker.h"
#include "cloud_topics/level_one/maintenance/worker_manager.h"
#include "model/fundamental.h"
#include "test_utils/test.h"

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

using namespace cloud_topics;
using namespace std::chrono_literals;

class WorkerManagerTestFixture : public seastar_test {
public:
    ss::future<> start_workers(l1::worker_manager& manager) {
        co_await manager._workers.start(
          &manager,
          nullptr,
          nullptr,
          nullptr,
          ss::default_scheduling_group(),
          nullptr);
        co_await manager._workers.invoke_on_all(&l1::compaction_worker::start);
    }

    ss::future<l1::compaction_worker::worker_state>
    get_worker_state(l1::worker_manager& manager, ss::shard_id shard) {
        return manager._workers.invoke_on(
          shard,
          [](l1::compaction_worker& worker) { return worker._worker_state; });
    }

    ss::future<bool>
    work_fut_has_value(l1::worker_manager& manager, ss::shard_id shard) {
        return manager._workers.invoke_on(
          shard, [](l1::compaction_worker& worker) {
              return worker._work_fut.has_value();
          });
    }

    ss::future<l1::compaction_job_state>
    get_job_state(l1::worker_manager& manager, ss::shard_id shard) {
        return manager._workers.invoke_on(
          shard,
          [](l1::compaction_worker& worker) { return worker._job_state; });
    }
};

TEST_F(WorkerManagerTestFixture, PauseAndResumeWorkers) {
    l1::compaction_scheduler_probe probe;
    l1::log_compaction_queue pq;
    l1::log_leveling_queue lq;
    l1::worker_manager manager(
      pq, lq, nullptr, nullptr, nullptr, probe, nullptr);
    start_workers(manager).get();
    auto stop_manager = ss::defer([&manager] { manager.stop().get(); });
    using worker_state = l1::compaction_worker::worker_state;
    for (ss::shard_id i = 0; i < ss::this_smp_shard_count(); ++i) {
        // Workers start in active state
        ASSERT_EQ(get_worker_state(manager, i).get(), worker_state::active);
        ASSERT_TRUE(work_fut_has_value(manager, i).get());

        // Pause workers and expect to see state reflect that.
        manager.pause_worker(i).get();
        ASSERT_EQ(get_worker_state(manager, i).get(), worker_state::paused);
        ASSERT_FALSE(work_fut_has_value(manager, i).get());

        // Resume workers and expect to see active state.
        manager.resume_worker(i).get();
        ASSERT_EQ(get_worker_state(manager, i).get(), worker_state::active);
        ASSERT_TRUE(work_fut_has_value(manager, i).get());
    }
}

TEST_F(WorkerManagerTestFixture, AcquireWork) {
    auto cmp_func = [](
                      const l1::log_compaction_meta_ptr& a,
                      const l1::log_compaction_meta_ptr& b) {
        return a->ntp < b->ntp;
    };

    l1::compaction_scheduler_probe probe;
    l1::log_compaction_queue pq(cmp_func);
    l1::log_leveling_queue lq(cmp_func);
    l1::log_list_t list;
    l1::worker_manager manager(
      pq, lq, nullptr, nullptr, nullptr, probe, nullptr);
    auto stop_manager = ss::defer([&manager] { manager.stop().get(); });

    const auto test_ntp = model::ntp(
      model::ns("kafka"), model::topic("tapioca"), model::partition_id(0));
    const auto test_tidp = model::topic_id_partition(
      model::topic_id(uuid_t::create()), test_ntp.tp.partition);
    auto meta = ss::make_lw_shared<l1::log_compaction_meta>(
      test_tidp, test_ntp);
    list.push_back(*meta);
    using state = l1::log_compaction_meta::log_state;
    meta->state = state::queued;
    pq.emplace(meta);

    auto work_opt = manager.try_acquire_work(ss::this_shard_id());
    ASSERT_TRUE(work_opt.has_value());
    auto& [work, kind] = work_opt.value();
    ASSERT_EQ(kind, l1::job_kind::compaction);
    ASSERT_EQ(work->ntp, test_ntp);
    ASSERT_EQ(work->tidp, test_tidp);
    ASSERT_TRUE(work->inflight_shard.has_value());
    ASSERT_EQ(work->state, state::inflight);
    ASSERT_EQ(work->inflight_shard.value(), ss::this_shard_id());

    manager.complete_work(work.get(), kind);
    ASSERT_FALSE(work->inflight_shard.has_value());
    ASSERT_EQ(work->state, state::idle);
}

TEST_F(WorkerManagerTestFixture, AcquireLevelingWork) {
    auto cmp_func = [](
                      const l1::log_compaction_meta_ptr& a,
                      const l1::log_compaction_meta_ptr& b) {
        return a->ntp < b->ntp;
    };

    l1::compaction_scheduler_probe probe;
    l1::log_compaction_queue pq(cmp_func);
    l1::log_leveling_queue lq(cmp_func);
    l1::log_list_t list;
    l1::worker_manager manager(
      pq, lq, nullptr, nullptr, nullptr, probe, nullptr);
    auto stop_manager = ss::defer([&manager] { manager.stop().get(); });

    const auto test_ntp = model::ntp(
      model::ns("kafka"), model::topic("tapioca"), model::partition_id(0));
    const auto test_tidp = model::topic_id_partition(
      model::topic_id(uuid_t::create()), test_ntp.tp.partition);
    auto meta = ss::make_lw_shared<l1::log_compaction_meta>(
      test_tidp, test_ntp);
    list.push_back(*meta);
    using state = l1::log_compaction_meta::log_state;
    meta->state = state::queued;
    lq.emplace(meta);

    auto work_opt = manager.try_acquire_work(ss::this_shard_id());
    ASSERT_TRUE(work_opt.has_value());
    auto& [work, kind] = work_opt.value();
    ASSERT_EQ(kind, l1::job_kind::leveling);
    ASSERT_EQ(work->ntp, test_ntp);
    ASSERT_EQ(work->tidp, test_tidp);
    ASSERT_TRUE(work->inflight_shard.has_value());
    ASSERT_EQ(work->state, state::inflight);
    ASSERT_EQ(work->inflight_shard.value(), ss::this_shard_id());

    manager.complete_work(work.get(), kind);
    ASSERT_FALSE(work->inflight_shard.has_value());
    ASSERT_EQ(work->state, state::idle);
}

// Verifies compaction has priority when both queues have work for the same
// partition: a compaction job is dispatched first; only after it completes
// does the leveling job become available.
TEST_F(WorkerManagerTestFixture, CompactionHasPriorityOverLeveling) {
    auto cmp_func = [](
                      const l1::log_compaction_meta_ptr& a,
                      const l1::log_compaction_meta_ptr& b) {
        return a->ntp < b->ntp;
    };

    l1::compaction_scheduler_probe probe;
    l1::log_compaction_queue pq(cmp_func);
    l1::log_leveling_queue lq(cmp_func);
    l1::log_list_t list;
    l1::worker_manager manager(
      pq, lq, nullptr, nullptr, nullptr, probe, nullptr);
    auto stop_manager = ss::defer([&manager] { manager.stop().get(); });

    const auto comp_ntp = model::ntp(
      model::ns("kafka"), model::topic("comp"), model::partition_id(0));
    const auto comp_tidp = model::topic_id_partition(
      model::topic_id(uuid_t::create()), comp_ntp.tp.partition);
    auto comp_meta = ss::make_lw_shared<l1::log_compaction_meta>(
      comp_tidp, comp_ntp);
    list.push_back(*comp_meta);

    const auto lvl_ntp = model::ntp(
      model::ns("kafka"), model::topic("lvl"), model::partition_id(0));
    const auto lvl_tidp = model::topic_id_partition(
      model::topic_id(uuid_t::create()), lvl_ntp.tp.partition);
    auto lvl_meta = ss::make_lw_shared<l1::log_compaction_meta>(
      lvl_tidp, lvl_ntp);
    list.push_back(*lvl_meta);

    using state = l1::log_compaction_meta::log_state;
    comp_meta->state = state::queued;
    lvl_meta->state = state::queued;
    pq.emplace(comp_meta);
    lq.emplace(lvl_meta);

    // First acquire returns the compaction job.
    auto first = manager.try_acquire_work(ss::this_shard_id());
    ASSERT_TRUE(first.has_value());
    ASSERT_EQ(first.value().second, l1::job_kind::compaction);
    ASSERT_EQ(first.value().first->ntp, comp_ntp);

    // Second acquire returns the leveling job.
    auto second = manager.try_acquire_work(ss::this_shard_id());
    ASSERT_TRUE(second.has_value());
    ASSERT_EQ(second.value().second, l1::job_kind::leveling);
    ASSERT_EQ(second.value().first->ntp, lvl_ntp);

    // Third acquire returns nothing.
    auto third = manager.try_acquire_work(ss::this_shard_id());
    ASSERT_FALSE(third.has_value());

    manager.complete_work(first.value().first.get(), l1::job_kind::compaction);
    manager.complete_work(second.value().first.get(), l1::job_kind::leveling);
}

// Verifies that interrupt_leveling_job sends a soft-stop to the worker on the
// target shard. This is the primitive the scheduler uses when a leveling job
// should be preempted because compaction has become eligible.
TEST_F(
  WorkerManagerTestFixture, PreemptsLevelingWhenCompactionBecomesEligible) {
    auto cmp_func = [](
                      const l1::log_compaction_meta_ptr& a,
                      const l1::log_compaction_meta_ptr& b) {
        return a->ntp < b->ntp;
    };

    l1::compaction_scheduler_probe probe;
    l1::log_compaction_queue pq(cmp_func);
    l1::log_leveling_queue lq(cmp_func);
    l1::log_list_t list;
    l1::worker_manager manager(
      pq, lq, nullptr, nullptr, nullptr, probe, nullptr);
    start_workers(manager).get();
    auto stop_manager = ss::defer([&manager] { manager.stop().get(); });

    const auto test_ntp = model::ntp(
      model::ns("kafka"), model::topic("tapioca"), model::partition_id(0));
    const auto test_tidp = model::topic_id_partition(
      model::topic_id(uuid_t::create()), test_ntp.tp.partition);
    auto meta = ss::make_lw_shared<l1::log_compaction_meta>(
      test_tidp, test_ntp);
    list.push_back(*meta);

    // Simulate the log being acquired as a leveling job: set state as the
    // worker_manager's try_acquire_work() would.
    using state = l1::log_compaction_meta::log_state;
    meta->state = state::queued;
    lq.emplace(meta);
    auto work_opt = manager.try_acquire_work(ss::this_shard_id());
    ASSERT_TRUE(work_opt.has_value());
    auto& [work, kind] = work_opt.value();
    ASSERT_EQ(kind, l1::job_kind::leveling);
    ASSERT_EQ(work->inflight_kind, l1::job_kind::leveling);
    ASSERT_EQ(work->state, state::inflight);

    // Before preemption the worker's job state is idle (no real job is
    // running; we are just checking the soft-stop signal path).
    ASSERT_EQ(
      get_job_state(manager, ss::this_shard_id()).get(),
      l1::compaction_job_state::idle);

    // Trigger preemption: compaction has become eligible for this log.
    manager.interrupt_leveling_job(*work->inflight_shard).get();

    // The worker on the inflight shard should now be soft-stopped.
    ASSERT_EQ(
      get_job_state(manager, ss::this_shard_id()).get(),
      l1::compaction_job_state::soft_stop);

    manager.complete_work(work.get(), kind);
}
