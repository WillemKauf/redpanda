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
#include "compaction/key_offset_map.h"
#include "resource_mgmt/memory_groups.h"
#include "serde/rw/envelope.h"
#include "storage/compaction/compaction_sink.h"
#include "storage/compaction/compaction_source.h"
#include "storage/disk_log_impl.h"

#include <seastar/core/file-types.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/seastar.hh>

namespace storage::local_compaction {

static ss::logger worker_log("storage-compaction-worker");

compaction_worker::compaction_worker() = default;

ss::future<> compaction_worker::initialize_map() {
    if (_map) {
        co_return;
    }
    auto size = memory_groups().compaction_reserved_memory();
    _map = std::make_unique<compaction::hash_key_offset_map>();
    co_await _map->initialize(size);
}

ss::future<> compaction_worker::persist_compaction_state(
  const std::filesystem::path& partition_dir, const compaction_state& state) {
    auto tmp_path = partition_dir / "compaction_state.tmp";
    auto final_path = partition_dir / "compaction_state";

    auto buf = serde::to_iobuf(state);

    auto file = co_await ss::open_file_dma(
      tmp_path.string(),
      ss::open_flags::wo | ss::open_flags::create | ss::open_flags::truncate);
    auto out = co_await ss::make_file_output_stream(std::move(file));
    for (const auto& frag : buf) {
        co_await out.write(frag.get(), frag.size());
    }
    co_await out.flush();
    co_await out.close();

    co_await ss::rename_file(tmp_path.string(), final_path.string());
}

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

    co_await initialize_map();
    co_await _map->reset();

    compaction_source src(
      log,
      *_map,
      std::move(info.dirty_ranges),
      info.removable_tombstone_ranges,
      cfg);
    compaction_sink snk(
      log,
      info.removable_tombstone_ranges,
      log.max_compacted_segment_size(),
      *cfg.asrc);

    // Run the sliding window algorithm manually so that src and snk remain
    // alive after finalize(), allowing us to read their results.
    std::exception_ptr eptr;
    try {
        co_await src.initialize();
        co_await ss::repeat([&src]() { return src.map_building_iteration(); });
        bool should_dedup = co_await snk.initialize(src);
        if (should_dedup) {
            co_await ss::repeat(
              [&src, &snk]() { return src.deduplication_iteration(snk); });
        }
    } catch (...) {
        eptr = std::current_exception();
    }
    co_await snk.finalize(eptr == nullptr);
    if (eptr) {
        std::rethrow_exception(eptr);
    }

    for (const auto& cr : snk.new_cleaned_ranges()) {
        state.cleaned_ranges.insert(cr.base_offset, cr.last_offset);
        if (cr.has_tombstones) {
            state.add(
              compaction_state::cleaned_range_with_tombstones{
                .base_offset = cr.base_offset,
                .last_offset = cr.last_offset,
                .cleaned_with_tombstones_at = model::timestamp::now(),
              });
        }
    }

    auto processed_stream = snk.processed_ranges().make_stream();
    while (processed_stream.has_next()) {
        auto interval = processed_stream.next();
        state.erase_contiguous_range_with_tombstones(
          interval.base_offset, interval.last_offset);
    }

    auto partition_dir = std::filesystem::path(log.config().work_directory());
    co_await persist_compaction_state(partition_dir, state);
}

} // namespace storage::local_compaction
