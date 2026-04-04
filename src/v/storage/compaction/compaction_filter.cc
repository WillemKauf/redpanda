// Copyright 2026 Redpanda Data, Inc.
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
#include "storage/logger.h"
#include "storage/types.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <optional>
#include <vector>

namespace storage::local_compaction {

compaction_filter::compaction_filter(
  compaction::sliding_window_reducer::sink& sink,
  const compaction::key_offset_map& map,
  model::ntp ntp,
  model::offset segment_last_offset,
  const model_offset_interval_set& removable_tombstone_ranges,
  const model_offset_interval_set& removable_transaction_ranges,
  ss::lw_shared_ptr<storage::stm_hookset> stm_mgr)
  : filter(sink, std::move(ntp))
  , _map(map)
  , _segment_last_offset(segment_last_offset)
  , _removable_tombstone_ranges(removable_tombstone_ranges)
  , _removable_transaction_ranges(removable_transaction_ranges)
  , _stm_mgr(std::move(stm_mgr)) {}

ss::future<bool> compaction_filter::should_keep(
  const model::record_batch& b, const model::record& r) const {
    if (compaction::is_removable_control_batch(b.header().type)) {
        co_return false;
    }

    if (r.is_tombstone()) {
        auto o = b.base_offset() + model::offset_delta(r.offset_delta());
        if (_removable_tombstone_ranges.contains(o)) {
            ++_stats.expired_tombstones_discarded;
            co_return false;
        }
    }

    if (b.header().attrs.is_control()) {
        auto o = b.base_offset() + model::offset_delta(r.offset_delta());
        if (_removable_transaction_ranges.contains(o)) {
            co_return false;
        }
    }

    auto keep = co_await compaction::is_latest_record_for_key(_map, b, r);

    co_return keep;
}

ss::future<std::optional<model::record_batch>>
compaction_filter::filter_batch(model::record_batch b) const {
    auto& hdr = b.header();
    auto t = hdr.type;
    const auto is_last_batch_in_segment = b.last_offset()
                                          == _segment_last_offset;
    // Compaction placeholder batches can actually be removed under two
    // conditions- that they are _not_ the last batch in a segment, nor are they
    // the last batch for an idempotent producer. The former can happen if e.g.
    // two segments containing placeholder batches are adjacently merged.
    if (
      (t == model::record_batch_type::compaction_placeholder)
      && !is_last_batch_in_segment
      && !_stm_mgr->is_batch_in_idempotent_window(hdr)) {
        co_return std::nullopt;
    }

    // do not filter non-removable batch types under any circumstances
    if (!compaction::is_filterable(t)) {
        co_return std::move(b);
    }

    std::vector<int32_t> offset_deltas = co_await compute_offset_deltas_to_keep(
      b);

    if (offset_deltas.empty()) {
        auto is_batch_in_idempotent_window
          = _stm_mgr->is_batch_in_idempotent_window(hdr);
        if (is_last_batch_in_segment || is_batch_in_idempotent_window) {
            // Local storage requires contiguous offsets, so when all records in
            // a batch are filtered out we emit a placeholder batch to preserve
            // the offset range. We also must keep the last batch for an
            // idempotent producer, or risk removing important metadata about
            // producer sequences and epochs.
            auto placeholder = compaction::make_placeholder_batch(hdr);
            vlog(
              gclog.debug,
              "installing a placeholder {} for compacted batch: {}",
              placeholder,
              b);
            co_return std::move(placeholder);
        }
    }

    auto keep_all_records = offset_deltas.size()
                            == static_cast<size_t>(b.record_count());
    auto is_committed_tx_raft_data = t == model::record_batch_type::raft_data
                                     && hdr.attrs.is_transactional()
                                     && !hdr.attrs.is_control();
    if (is_committed_tx_raft_data) {
        vlog(gclog.trace, "Removing transactional bit for raft batch {}", hdr);
        hdr.attrs.remove_transactional_type();
        if (keep_all_records) {
            // Reset the header's checksum here, since there will be an early
            // return below in `do_filter_batch()` when all records are kept.
            hdr.reset_size_checksum_metadata(b.data());
        }
    }

    co_return co_await do_filter_batch(std::move(b), std::move(offset_deltas));
}

} // namespace storage::local_compaction
