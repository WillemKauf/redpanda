// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/compaction/compaction_worker.h"

#include "base/vlog.h"
#include "compaction/reducer.h"
#include "storage/compaction/compaction_sink.h"
#include "storage/compaction/compaction_source.h"
#include "storage/disk_log_impl.h"
#include "storage/probe.h"

#include <seastar/coroutine/as_future.hh>
#include <seastar/core/future.hh>

namespace storage::local_compaction {

static ss::logger worker_log("storage-compaction-worker");

ss::future<> compaction_worker::compact(
  disk_log_impl& log,
  compaction_state& state,
  const compaction::compaction_config& cfg) {
    auto info = state.get_compaction_info(
      cfg.max_removable_local_log_offset,
      cfg.max_tombstone_remove_offset,
      cfg.tombstone_retention_ms);

    if (info.dirty_ranges.empty() && info.removable_tombstone_ranges.empty()) {
        vlog(worker_log.debug, "nothing to compact for {}", log.config().ntp());
        co_return;
    }

    co_await cfg.hash_key_map->reset();

    auto src = std::make_unique<compaction_source>(
      log, std::move(info.dirty_ranges), info.removable_tombstone_ranges, cfg);
    auto snk = std::make_unique<compaction_sink>(
      log,
      state,
      info.removable_tombstone_ranges,
      log.max_compacted_segment_size(),
      *cfg.asrc);

    auto m = log.get_probe().auto_compaction_measurement();

    auto fut = co_await ss::coroutine::as_future(
      compaction::sliding_window_reducer(std::move(src), std::move(snk)).run());
    if (fut.failed()) {
        m->cancel();
        std::rethrow_exception(fut.get_exception());
    }
}

} // namespace storage::local_compaction
