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
#include "model/record_utils.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <optional>

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
  const model::record_batch_header& hdr, const model::record& r) const {
    if (r.is_tombstone()) {
        auto o = model::offset_cast(
          hdr.base_offset + model::offset_delta(r.offset_delta()));
        if (_removable_tombstone_ranges.contains(o)) {
            ++_stats.expired_tombstones_discarded;
            co_return false;
        }
    }

    auto keep = co_await compaction::is_latest_record_for_key(_map, hdr, r);

    co_return keep;
}

ss::future<> compaction_filter::maybe_index_offset_delta(
  const model::record_batch_header& hdr,
  const model::record& r,
  absl::flat_hash_set<int32_t>& offset_deltas) const {
    if (co_await should_keep(hdr, r)) {
        offset_deltas.insert(r.offset_delta());
    }
}

ss::future<absl::flat_hash_set<int32_t>>
compaction_filter::compute_offset_deltas_to_keep(
  const model::record_batch_header& hdr, iobuf records) const {
    absl::flat_hash_set<int32_t> offset_deltas;
    offset_deltas.reserve(hdr.record_count);

    iobuf_parser parser(std::move(records));
    for (int32_t i = 0; i < hdr.record_count; ++i) {
        auto record = model::parse_one_record_from_buffer(parser);
        co_await maybe_index_offset_delta(hdr, record, offset_deltas);
    }

    co_return offset_deltas;
}

ss::future<std::optional<model::record_batch>>
compaction_filter::filter_batch_with_offset_deltas(
  model::record_batch b, absl::flat_hash_set<int32_t> offset_deltas) const {
    co_return co_await do_filter_batch(std::move(b), std::move(offset_deltas));
}

} // namespace cloud_topics::l1
