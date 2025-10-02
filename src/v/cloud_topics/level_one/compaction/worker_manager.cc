/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/worker_manager.h"

#include "cloud_topics/level_one/common/file_io.h"
#include "cloud_topics/level_one/compaction/committer.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "cloud_topics/level_one/compaction/worker.h"
#include "cloud_topics/level_one/metastore/replicated_metastore.h"
#include "model/fundamental.h"

namespace cloud_topics::l1 {

worker_manager::worker_manager(
  pq_t& work_queue,
  ss::sharded<file_io>* io,
  ss::sharded<replicated_metastore>* metastore,
  ss::sharded<compaction_committer>* committer)
  : _work_queue(work_queue)
  , _io(io)
  , _metastore(metastore)
  , _committer(committer) {}

ss::future<> worker_manager::start() {
    co_await _workers.start(
      this,
      ss::sharded_parameter([this] { return &_io->local(); }),
      ss::sharded_parameter([this] { return &_metastore->local(); }),
      ss::sharded_parameter([this] { return &_committer->local(); }));
    co_await _workers.invoke_on_all(&compaction_worker::start);
}

ss::future<> worker_manager::stop() { co_await _workers.stop(); }

std::optional<log_compaction_meta*>
worker_manager::try_acquire_work(ss::shard_id worker_shard) {
    vassert(
      ss::this_shard_id() == worker_manager_shard,
      "Expected calls to worker_manager::try_acquire_work() to always "
      "execute on shard {}",
      worker_manager_shard);

    if (_work_queue.empty()) {
        return std::nullopt;
    }

    auto log = _work_queue.top();
    _work_queue.pop();

    if (!log) {
        return std::nullopt;
    }

    if (!log->link.is_linked()) {
        return std::nullopt;
    }

    _inflight_tidp_to_shard.insert_or_assign(log->tidp, worker_shard);
    log->inflight = true;
    return log;
}

void worker_manager::finish_work(
  log_compaction_meta* log, ss::shard_id worker_shard) {
    vassert(
      ss::this_shard_id() == worker_manager_shard,
      "Expected calls to worker_manager::finish_work() to always execute "
      "on shard {}",
      worker_manager_shard);

    if (!log) {
        return;
    }

    if (!log->link.is_linked()) {
        return;
    }

    auto tidp = log->tidp;
    auto it = _inflight_tidp_to_shard.extract(tidp);

    if (it->second != worker_shard) {
        vlog(
          compaction_log.error,
          "Concurrency issue for compaction of log {}- expected work to be "
          "finished on shard {}, but was finished on shard {}",
          tidp,
          it->second,
          worker_shard);
    }

    log->inflight = false;
}

ss::future<>
worker_manager::request_stop_compaction(model::topic_id_partition tidp) {
    auto it = _inflight_tidp_to_shard.find(tidp);
    if (it == _inflight_tidp_to_shard.end()) {
        co_return;
    }

    auto shard = it->second;
    co_await _workers.invoke_on(shard, [](compaction_worker& worker) {
        return worker.request_hard_stop();
    });
}

ss::future<> worker_manager::alert_workers() {
    co_await _workers.invoke_on_all(
      [](compaction_worker& worker) { worker.alert_worker(); });
}

ss::future<> worker_manager::pause_worker(ss::shard_id worker) {
    co_await _workers.invoke_on(
      worker, [](compaction_worker& worker) { return worker.pause_worker(); });
}

ss::future<> worker_manager::resume_worker(ss::shard_id worker) {
    co_await _workers.invoke_on(
      worker, [](compaction_worker& worker) { return worker.resume_worker(); });
}

} // namespace cloud_topics::l1
