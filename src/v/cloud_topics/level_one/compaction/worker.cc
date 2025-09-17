/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/worker.h"

#include "cloud_topics/level_one/compaction/logger.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "cloud_topics/level_one/compaction/sink.h"
#include "cloud_topics/level_one/compaction/source.h"
#include "compaction/reducer.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "ssx/future-util.h"

#include <seastar/coroutine/as_future.hh>

namespace cloud_topics::l1 {

compaction_worker::compaction_worker(
  io* io, metastore* metastore, compaction_committer* committer)
  : _io(io)
  , _metastore(metastore)
  , _committer(committer) {}

ss::future<> compaction_worker::compact(
  model::ntp ntp,
  model::topic_id_partition tidp,
  metastore::compaction_offsets_response compaction_offsets,
  ss::abort_source& as) {
    if (_stopped) {
        co_return;
    }

    // If there was a concurrent race with a request to cancel/stop an inflight
    // compaction, early return after resetting state to `idle`.
    if (
      _state == compaction_job_state::soft_stop
      || _state == compaction_job_state::hard_stop) {
        _state = compaction_job_state::idle;
        co_return;
    }

    vlog(compaction_log.info, "Compacting CTP {}", tidp);

    _state = compaction_job_state::running;

    // Lazy initialization of offset map.
    if (!_map) {
        co_await initialize_map();
    }

    auto src = std::make_unique<compaction_source>(
      std::move(ntp),
      tidp,
      std::move(compaction_offsets),
      _map.get(),
      _metastore,
      _io,
      as,
      _state);
    auto sink = std::make_unique<compaction_sink>(_io, _committer, tidp);
    auto reducer = compaction::sliding_window_reducer(
      std::move(src), std::move(sink));

    auto compact_fut = co_await ss::coroutine::as_future(
      std::move(reducer).run());

    if (compact_fut.failed()) {
        auto eptr = compact_fut.get_exception();
        auto log_lvl = ssx::is_shutdown_exception(eptr) ? ss::log_level::debug
                                                        : ss::log_level::warn;
        vlogl(
          compaction_log,
          log_lvl,
          "Caught exception {} while compacting CTP {}.",
          eptr,
          tidp);
    }

    _state = compaction_job_state::idle;
}

void compaction_worker::request_hard_stop() {
    _state = compaction_job_state::hard_stop;
}

void compaction_worker::request_soft_stop() {
    _state = compaction_job_state::soft_stop;
}

void compaction_worker::stop_worker() {
    request_hard_stop();
    _stopped = true;
}

ss::future<> compaction_worker::initialize_map() {
    if (_map) {
        co_return;
    }

    // TODO: use memory group reservation.
    // auto compaction_mem_bytes = memory_groups().compaction_reserved_memory();
    auto compaction_mem_bytes
      = config::shard_local_cfg().storage_compaction_key_map_memory();
    auto compaction_map = std::make_unique<compaction::hash_key_offset_map>();
    co_await compaction_map->initialize(compaction_mem_bytes);
    _map = std::move(compaction_map);
}

} // namespace cloud_topics::l1
