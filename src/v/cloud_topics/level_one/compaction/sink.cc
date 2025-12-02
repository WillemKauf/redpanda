/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/sink.h"

#include "bytes/iostream.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/compaction/committer.h"
#include "cloud_topics/level_one/compaction/logger.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "cloud_topics/level_one/compaction/source.h"
#include "cloud_topics/level_one/metastore/offset_interval_set.h"
#include "compaction/reducer.h"
#include "model/batch_compression.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "ssx/future-util.h"

#include <seastar/coroutine/as_future.hh>

#include <exception>

namespace cloud_topics::l1 {

namespace {

offset_interval_set get_removed_tombstone_ranges(
  const offset_interval_set& removable_tombstone_ranges,
  const offset_interval_set& processed_extents) {
    offset_interval_set removed_tombstone_ranges;
    auto stream = removable_tombstone_ranges.make_stream();
    while (stream.has_next()) {
        auto i = stream.next();
        if (processed_extents.covers(i.base_offset, i.last_offset)) {
            removed_tombstone_ranges.insert(i.base_offset, i.last_offset);
        }
    }
    return removed_tombstone_ranges;
}

chunked_vector<metastore::compaction_update::cleaned_range>
get_new_cleaned_ranges(
  const chunked_vector<metastore::compaction_update::cleaned_range>&
    maybe_cleaned_ranges,
  const offset_interval_set& processed_extents) {
    chunked_vector<metastore::compaction_update::cleaned_range>
      new_cleaned_ranges;
    new_cleaned_ranges.reserve(maybe_cleaned_ranges.size());
    for (const auto& cleaned_range : maybe_cleaned_ranges) {
        if (processed_extents.covers(
              cleaned_range.base_offset, cleaned_range.last_offset)) {
            new_cleaned_ranges.push_back(cleaned_range);
        }
    }

    new_cleaned_ranges.shrink_to_fit();
    return new_cleaned_ranges;
}

} // namespace

compaction_sink::compaction_sink(
  model::topic_id_partition tp,
  const chunked_vector<offset_interval_set::interval>& dirty_range_intervals,
  const offset_interval_set& removable_tombstone_ranges,
  io* io,
  compaction_committer* committer,
  object_builder::options opts)
  : _tp(tp)
  , _dirty_range_intervals(dirty_range_intervals)
  , _removable_tombstone_ranges(removable_tombstone_ranges)
  , _io(io)
  , _committer(committer)
  , _opts(opts) {}

ss::future<bool>
compaction_sink::initialize(compaction::sliding_window_reducer::source& src) {
    auto& ct_src = static_cast<compaction_source&>(src);

    bool has_removable_tombstones = !_removable_tombstone_ranges.empty();
    bool has_dirty_ranges = !_dirty_range_intervals.empty();
    bool should_compact = !ct_src._extents.empty()
                          && (has_removable_tombstones || has_dirty_ranges);

    if (!should_compact) {
        co_return false;
    }

    co_await initialize_builder();

    auto& new_cleaned_ranges = ct_src._new_cleaned_ranges;
    new_cleaned_ranges.shrink_to_fit();
    _new_cleaned_ranges = std::move(new_cleaned_ranges);

    vlog(
      compaction_log.debug,
      "Built compaction map for tidp {}, with {} keys (max allowed "
      "{})",
      _tp,
      ct_src._map->size(),
      ct_src._map->capacity());

    co_return true;
}

bool compaction_sink::needs_roll() const {
    if (!_active_staging_file) {
        return true;
    }

    if (_builder->file_size() >= max_object_size) {
        return true;
    }

    return false;
}

ss::future<> compaction_sink::initialize_builder() {
    auto staging_file_fut = co_await ss::coroutine::as_future(
      _io->create_tmp_file());

    if (staging_file_fut.failed()) {
        auto e = staging_file_fut.get_exception();
        vlogl(
          compaction_log,
          ssx::is_shutdown_exception(e) ? ss::log_level::warn
                                        : ss::log_level::error,
          "Exception creating staging file: {}",
          e);
        std::rethrow_exception(e);
    }
    auto staging_file_result = staging_file_fut.get();

    _active_staging_file = std::move(staging_file_result).value();
    auto output_stream = co_await _active_staging_file->output_stream();

    _builder = object_builder::create(std::move(output_stream), _opts);

    co_await _builder->start_partition(_tp);
}

ss::future<> compaction_sink::roll(bool initialize_new_builder) {
    // 1. Push currently built L1 object & metadata to the committer.
    if (_active_staging_file) {
        auto active_staging_file = std::exchange(_active_staging_file, nullptr);
        auto builder = std::exchange(_builder, nullptr);

        auto object_info_fut = co_await ss::coroutine::as_future(
          builder->finish());
        co_await builder->close();
        if (object_info_fut.failed()) {
            auto e = object_info_fut.get_exception();
            vlogl(
              compaction_log,
              ssx::is_shutdown_exception(e) ? ss::log_level::warn
                                            : ss::log_level::error,
              "Exception creating object_info: {}. Exiting compaction early.",
              e);
            co_await active_staging_file->remove();
            std::rethrow_exception(e);
        }

        auto object_info = object_info_fut.get();
        auto ntp_md = [this](const object_builder::object_info& info) {
            auto [first, last] = info.index.partitions.equal_range(_tp);
            vassert(
              std::distance(first, last) == 1,
              "Expected one partition range in builder.");
            return metastore::object_metadata::ntp_metadata{
              .tidp = _tp,
              .base_offset = _object_base_offset,
              .last_offset = _last_processed_offset,
              .max_timestamp = first->second.max_timestamp,
              .pos = first->second.file_position,
              .size = first->second.length};
        }(object_info);

        auto file_and_info = file_and_md_info{
          .staging_file = std::move(active_staging_file),
          .info = std::move(object_info),
          .ntp_md = std::move(ntp_md)};

        // TODO: push update to committer.
        std::ignore = std::move(file_and_info);

        _object_base_offset = kafka::next_offset(_last_processed_offset);
    }

    // 2. Start new `_active_staging_file` and `_builder`.
    if (initialize_new_builder) {
        co_await initialize_builder();
    }
}

ss::future<> compaction_sink::maybe_roll() {
    if (needs_roll()) {
        co_await roll(true);
    }
}

ss::future<ss::stop_iteration>
compaction_sink::operator()(model::record_batch b, model::compression c) {
    auto prev_offset = std::max(
      kafka::prev_offset(model::offset_cast(b.base_offset())),
      kafka::offset{0});
    set_last_processed_offset(prev_offset);

    co_await maybe_roll();

    if (c != model::compression::none) {
        b = co_await model::compress_batch(c, std::move(b));
    }

    co_await _builder->add_batch(std::move(b));

    co_return ss::stop_iteration::no;
}

ss::future<> compaction_sink::process_next_extent_offset_bounds(
  kafka::offset next_extent_base, kafka::offset next_extent_last) {
    bool is_first_extent = _object_base_offset == kafka::offset{};
    if (is_first_extent) {
        _object_base_offset = next_extent_base;
    } else {
        _processed_extents.insert(_extent_base_offset, _extent_last_offset);
        set_last_processed_offset(_extent_last_offset);
        if (next_extent_base != kafka::next_offset(_extent_last_offset)) {
            // Passed extents are non-contiguous. Force a roll of the
            // currently built L1 object with previous extent's last offset.
            co_await roll(true);
        }
    }

    _extent_base_offset = next_extent_base;
    _extent_last_offset = next_extent_last;
}

ss::future<> compaction_sink::finalize() {
    _processed_extents.insert(_extent_base_offset, _extent_last_offset);
    set_last_processed_offset(_extent_last_offset);

    co_await roll(false);

    auto removed_tombstone_ranges = get_removed_tombstone_ranges(
      _removable_tombstone_ranges, _processed_extents);
    auto new_cleaned_ranges = get_new_cleaned_ranges(
      _new_cleaned_ranges, _processed_extents);

    // TODO: finalize job with committer
    std::ignore = std::move(removed_tombstone_ranges);
    std::ignore = std::move(new_cleaned_ranges);
}

} // namespace cloud_topics::l1
