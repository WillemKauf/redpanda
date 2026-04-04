/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/filter.h"

#include "compaction/utils.h"
#include "model/fundamental.h"
#include "model/record.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <optional>
#include <vector>

namespace cloud_topics::l1 {

compaction_filter::compaction_filter(
  compaction::sliding_window_reducer::sink& sink,
  const compaction::key_offset_map& map,
  model::ntp ntp,
  const offset_interval_set& removable_tombstone_ranges)
  : filter(sink, std::move(ntp))
  , _map(map)
  , _removable_tombstone_ranges(removable_tombstone_ranges) {}

ss::future<bool> compaction_filter::should_keep(
  const model::record_batch& b, const model::record& r) const {
    if (r.is_tombstone()) {
        auto o = model::offset_cast(
          b.base_offset() + model::offset_delta(r.offset_delta()));
        if (_removable_tombstone_ranges.contains(o)) {
            ++_stats.expired_tombstones_discarded;
            co_return false;
        }
    }

    auto keep = co_await compaction::is_latest_record_for_key(_map, b, r);

    co_return keep;
}

ss::future<std::optional<model::record_batch>>
compaction_filter::filter_batch(model::record_batch b) const {
    // compute which records to keep
    std::vector<int32_t> offset_deltas = co_await compute_offset_deltas_to_keep(
      b);

    auto ret = co_await do_filter_batch(std::move(b), std::move(offset_deltas));
    co_return ret;
}

} // namespace cloud_topics::l1
