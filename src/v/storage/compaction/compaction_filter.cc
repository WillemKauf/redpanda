// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/compaction/compaction_filter.h"

#include "compaction/utils.h"
#include "model/fundamental.h"
#include "model/record.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <optional>
#include <vector>

namespace storage::local_compaction {

compaction_filter::compaction_filter(
  compaction::sliding_window_reducer::sink& sink,
  const compaction::key_offset_map& map,
  model::ntp ntp,
  const model_offset_interval_set& removable_tombstone_ranges)
  : filter(sink, std::move(ntp))
  , _map(map)
  , _removable_tombstone_ranges(removable_tombstone_ranges) {}

ss::future<bool> compaction_filter::should_keep(
  const model::record_batch& b, const model::record& r) const {
    if (r.is_tombstone()) {
        auto o = b.base_offset() + model::offset_delta(r.offset_delta());
        if (_removable_tombstone_ranges.contains(o)) {
            ++_stats.expired_tombstones_discarded;
            co_return false;
        }
    }

    auto keep = co_await compaction::is_latest_record_for_key(_map, b, r);

    co_return keep;
}

ss::future<std::vector<int32_t>>
compaction_filter::compute_offset_deltas_to_keep(
  const model::record_batch& b) const {
    std::vector<int32_t> offset_deltas;
    offset_deltas.reserve(b.record_count());

    co_await b.for_each_record_async(
      [this, &b, &offset_deltas](const model::record& r) {
          return should_keep(b, r).then([&offset_deltas, &r](bool keep) {
              if (keep) {
                  offset_deltas.push_back(r.offset_delta());
              }
          });
      });

    co_return offset_deltas;
}

ss::future<std::optional<model::record_batch>>
compaction_filter::filter_batch_with_offset_deltas(
  model::record_batch b, std::vector<int32_t> offset_deltas) const {
    if (offset_deltas.empty()) {
        // Local storage requires contiguous offsets, so when all records in a
        // batch are filtered out we emit a placeholder batch to preserve the
        // offset range.
        co_return compaction::make_placeholder_batch(b.header());
    }
    co_return co_await do_filter_batch(std::move(b), std::move(offset_deltas));
}

} // namespace storage::local_compaction
