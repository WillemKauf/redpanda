// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/compaction/compaction_sink.h"

#include "base/seastarx.h"
#include "base/vassert.h"
#include "base/vlog.h"
#include "model/batch_compression.h"
#include "model/fundamental.h"
#include "serde/rw/envelope.h"
#include "storage/disk_log_impl.h"
#include "storage/kvstore.h"
#include "storage/segment.h"
#include "storage/segment_utils.h"

#include <seastar/coroutine/as_future.hh>
#include <seastar/util/log.hh>

#include <limits>

namespace storage::local_compaction {

static ss::logger sink_log("storage-compaction-sink");

compaction_sink::compaction_sink(
  disk_log_impl& log,
  compaction_state& state,
  model_offset_interval_set removable_tombstone_ranges,
  size_t max_compacted_segment_size,
  ss::abort_source& as)
  : _log(log)
  , _state(state)
  , _removable_tombstone_ranges(std::move(removable_tombstone_ranges))
  , _max_compacted_segment_size(max_compacted_segment_size)
  , _as(as) {}

ss::future<bool> compaction_sink::initialize(
  compaction::sliding_window_reducer::source& /*src*/) {
    // The source will have accumulated new_cleaned_ranges during map building.
    // The worker is responsible for moving them here after the source finishes
    // map building. For now, simply return true to proceed with dedup if the
    // log has segments.
    co_return !_log.segments().empty();
}

ss::future<> compaction_sink::prepare_iteration(kafka::offset base) {
    auto model_base = model::offset{base()};

    if (!_staging_segment) {
        // First iteration or after a roll committed the previous segment.
        _range_start = model_base;
        co_return co_await open_staging_segment(model_base, _current_term);
    }

    if (_current_term != _staging_term) {
        // Term changed: roll the current segment and start a new one.
        vlog(
          sink_log.debug,
          "Term change {} -> {} at offset {}, rolling staging segment for "
          "ntp {}",
          _staging_term,
          _current_term,
          model_base,
          _log.config().ntp());
        co_await roll();
        _range_start = model_base;
        co_await open_staging_segment(model_base, _current_term);
    }

    // Otherwise continue appending to the current staging segment.
}

ss::future<ss::stop_iteration>
compaction_sink::operator()(model::record_batch b, model::compression c) {
    vassert(
      _staging_segment && _staging_segment->has_appender(),
      "compaction_sink::operator() called without an active staging segment "
      "for ntp {}",
      _log.config().ntp());

    if (_as.abort_requested()) {
        co_return ss::stop_iteration::yes;
    }

    // Check if we need to roll due to size.
    if (_staging_segment->size_bytes() >= _max_compacted_segment_size) {
        auto next_base = b.base_offset();
        co_await roll();
        _range_start = next_base;
        co_await open_staging_segment(next_base, _current_term);
    }

    // Check if the offset span would overflow uint32_t. The offset index
    // uses 32-bit relative offsets, so a single segment cannot span more
    // than ~4 billion offsets.
    auto span = b.last_offset()() - _range_start();
    if (span > std::numeric_limits<uint32_t>::max()) {
        auto next_base = b.base_offset();
        co_await roll();
        _range_start = next_base;
        co_await open_staging_segment(next_base, _current_term);
    }

    // Re-compress if needed before appending.
    if (c != model::compression::none) {
        b = co_await model::compress_batch(c, std::move(b));
    }

    co_await _staging_segment->append(std::move(b));

    co_return ss::stop_iteration::no;
}

ss::future<>
compaction_sink::finish_iteration(kafka::offset base, kafka::offset last) {
    _range_end = model::offset{last()};
    _processed_ranges.insert(model::offset{base()}, model::offset{last()});
    co_return;
}

ss::future<> compaction_sink::finalize(bool success) {
    if (!success) {
        co_return co_await discard_inflight();
    }

    if (_staging_segment) {
        co_await roll();
    }

    // Apply results to compaction_state and persist.
    for (const auto& cr : _new_cleaned_ranges) {
        _state.cleaned_ranges.insert(cr.base_offset, cr.last_offset);
        if (cr.has_tombstones) {
            _state.add(
              compaction_state::cleaned_range_with_tombstones{
                .base_offset = cr.base_offset,
                .last_offset = cr.last_offset,
                .cleaned_with_tombstones_at = model::timestamp::now(),
              });
        }
    }

    auto processed_stream = _processed_ranges.make_stream();
    while (processed_stream.has_next()) {
        auto interval = processed_stream.next();
        _state.erase_contiguous_range_with_tombstones(
          interval.base_offset, interval.last_offset);
    }

    co_await _log.kv_store().put(
      kvstore::key_space::storage,
      internal::compaction_state_key(_log.config().ntp()),
      serde::to_iobuf(_state));
}

ss::future<>
compaction_sink::open_staging_segment(model::offset base, model::term_id term) {
    _staging_term = term;
    _staging_segment = co_await _log.make_segment(
      base, term, _max_compacted_segment_size);
    vlog(
      sink_log.trace,
      "Opened staging segment at offset {} term {} for ntp {}",
      base,
      term,
      _log.config().ntp());
}

ss::future<> compaction_sink::roll() {
    vassert(
      _staging_segment,
      "compaction_sink::roll() called without an active staging segment for "
      "ntp {}",
      _log.config().ntp());

    // Flush and close the appender but keep the segment alive for
    // replace_offset_range.
    co_await _staging_segment->flush();

    auto replacement = std::exchange(_staging_segment, nullptr);

    vlog(
      sink_log.debug,
      "Rolling staging segment, replacing range [{}, {}] for ntp {}",
      _range_start,
      _range_end,
      _log.config().ntp());

    co_await _log.replace_offset_range(
      _range_start, _range_end, std::move(replacement));
}

ss::future<> compaction_sink::discard_inflight() {
    if (!_staging_segment) {
        co_return;
    }

    auto seg = std::exchange(_staging_segment, nullptr);
    auto close_fut = co_await ss::coroutine::as_future(seg->close());
    if (close_fut.failed()) {
        auto e = close_fut.get_exception();
        vlog(
          sink_log.warn,
          "Error closing discarded staging segment for ntp {}: {}",
          _log.config().ntp(),
          e);
    }

    auto remove_fut = co_await ss::coroutine::as_future(
      seg->remove_persistent_state());
    if (remove_fut.failed()) {
        auto e = remove_fut.get_exception();
        vlog(
          sink_log.warn,
          "Error removing discarded staging segment files for ntp {}: {}",
          _log.config().ntp(),
          e);
    }
}

} // namespace storage::local_compaction
