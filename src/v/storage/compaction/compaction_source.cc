// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/compaction/compaction_source.h"

#include "compaction/key.h"
#include "compaction/key_offset_map.h"
#include "compaction/reducer.h"
#include "compaction/types.h"
#include "model/batch_compression.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "storage/compaction/compaction_filter.h"
#include "storage/compaction/compaction_sink.h"
#include "storage/disk_log_impl.h"
#include "storage/segment.h"
#include "storage/types.h"

#include <seastar/core/coroutine.hh>
#include <seastar/util/log.hh>

namespace storage::local_compaction {

namespace {

static ss::logger source_log("storage-compaction-source");

class map_building_reducer {
public:
    struct return_t {
        bool map_is_full;
        std::optional<model::offset> max_indexed_offset;
        bool range_has_tombstones;
    };

    explicit map_building_reducer(
      compaction::key_offset_map& map, model::offset start_offset)
      : _map(map)
      , _start_offset(start_offset) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch b) {
        if (b.compressed()) {
            b = co_await model::decompress_batch(b);
        }

        co_await b.for_each_record_async(
          [this, base_offset = b.base_offset()](
            const model::record& r) -> ss::future<ss::stop_iteration> {
              if (r.is_tombstone()) {
                  _range_has_tombstones = true;
              }
              return maybe_index_record_in_map(r, base_offset);
          });

        if (_map_is_full) {
            co_return ss::stop_iteration::yes;
        }

        co_return ss::stop_iteration::no;
    }

    return_t end_of_stream() {
        return {_map_is_full, _max_indexed_offset, _range_has_tombstones};
    }

private:
    ss::future<ss::stop_iteration> maybe_index_record_in_map(
      const model::record& r, model::offset base_offset) {
        auto offset = base_offset + model::offset_delta(r.offset_delta());

        if (offset < _start_offset) {
            co_return ss::stop_iteration::no;
        }

        auto key = compaction::compaction_key{iobuf_to_bytes(r.key())};
        bool inserted = co_await _map.put(key, offset);

        if (inserted) {
            _max_indexed_offset = model::offset(
              std::max(_max_indexed_offset.value_or(offset)(), offset()));
            co_return ss::stop_iteration::no;
        }

        _map_is_full = true;
        co_return ss::stop_iteration::yes;
    }

    compaction::key_offset_map& _map;
    model::offset _start_offset;

    bool _map_is_full{false};
    bool _range_has_tombstones{false};
    std::optional<model::offset> _max_indexed_offset{std::nullopt};
};

} // namespace

compaction_source::compaction_source(
  disk_log_impl& log,
  model_offset_interval_set dirty_ranges,
  model_offset_interval_set removable_tombstone_ranges,
  const compaction::compaction_config& cfg)
  : _log(log)
  , _dirty_ranges(std::move(dirty_ranges))
  , _removable_tombstone_ranges(std::move(removable_tombstone_ranges))
  , _cfg(cfg) {}

ss::future<> compaction_source::initialize() {
    _dirty_range_intervals = _dirty_ranges.to_vec();
    _dirty_range_it = _dirty_range_intervals.crbegin();
    co_return;
}

ss::future<ss::stop_iteration> compaction_source::map_building_iteration() {
    if (preempted()) {
        co_return ss::stop_iteration::yes;
    }

    if (_dirty_range_it == _dirty_range_intervals.crend()) {
        co_return ss::stop_iteration::yes;
    }

    const auto& dirty_range = *_dirty_range_it;

    local_log_reader_config reader_cfg(
      dirty_range.base_offset, dirty_range.last_offset, std::ref(*_cfg.asrc));
    reader_cfg.skip_batch_cache = true;

    auto rdr = co_await _log.make_reader(std::move(reader_cfg));

    auto res = co_await std::move(rdr).consume(
      map_building_reducer(*_cfg.hash_key_map, dirty_range.base_offset),
      model::no_timeout);
    bool map_is_full = res.map_is_full;
    auto max_indexed_offset = res.max_indexed_offset;

    if (max_indexed_offset.has_value()) {
        auto base_offset = dirty_range.base_offset;
        auto last_offset = map_is_full ? max_indexed_offset.value()
                                       : dirty_range.last_offset;
        vassert(
          base_offset <= last_offset,
          "Cleaned range must be properly bounded.");
        _new_cleaned_ranges.push_back(
          {.base_offset = base_offset,
           .last_offset = last_offset,
           .has_tombstones = res.range_has_tombstones});
    }

    if (map_is_full) {
        co_return ss::stop_iteration::yes;
    }

    ++_dirty_range_it;
    co_return ss::stop_iteration::no;
}

ss::future<ss::stop_iteration> compaction_source::deduplication_iteration(
  compaction::sliding_window_reducer::sink& sink) {
    if (preempted()) {
        co_return ss::stop_iteration::yes;
    }

    const auto& segs = _log.segments();
    if (_dedup_seg_idx >= segs.size()) {
        co_return ss::stop_iteration::yes;
    }

    auto seg = segs[_dedup_seg_idx];
    ++_dedup_seg_idx;

    auto seg_base = seg->offsets().get_base_offset();
    auto seg_last = seg->offsets().get_dirty_offset();

    // Skip segments beyond the compactible limit.
    if (seg_base > _cfg.max_removable_local_log_offset) {
        co_return ss::stop_iteration::yes;
    }

    // Clamp the last offset to the compactible limit.
    seg_last = std::min(seg_last, _cfg.max_removable_local_log_offset);

    auto& typed_sink = static_cast<compaction_sink&>(sink);
    typed_sink.set_current_term(seg->offsets().get_term());

    co_await sink.prepare_iteration(kafka::offset{seg_base()});

    local_log_reader_config reader_cfg(
      seg_base, seg_last, std::ref(*_cfg.asrc));
    reader_cfg.skip_batch_cache = true;

    auto rdr = co_await _log.make_reader(std::move(reader_cfg));
    auto stats = co_await rdr.consume(
      compaction_filter{
        sink,
        *_cfg.hash_key_map,
        _log.config().ntp(),
        _removable_tombstone_ranges},
      model::no_timeout);

    co_await sink.finish_iteration(
      kafka::offset{seg_base()}, kafka::offset{seg_last()});

    if (stats.has_removed_data()) {
        vlog(
          source_log.info,
          "Local compaction removing data from {}, offset range ({}~{}), "
          "stats: {}",
          _log.config().ntp(),
          seg_base,
          seg_last,
          stats);
    } else {
        vlog(
          source_log.debug,
          "Local compaction not removing data from {}, offset range "
          "({}~{}), stats: {}",
          _log.config().ntp(),
          seg_base,
          seg_last,
          stats);
    }

    co_return ss::stop_iteration::no;
}

bool compaction_source::preempted() const {
    return _cfg.asrc && _cfg.asrc->abort_requested();
}

} // namespace storage::local_compaction
