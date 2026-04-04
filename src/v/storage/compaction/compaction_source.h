/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "compaction/key_offset_map.h"
#include "compaction/reducer.h"
#include "compaction/types.h"
#include "container/offset_interval_set.h"
#include "model/fundamental.h"
#include "storage/compaction/compaction_state.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

namespace storage {
class disk_log_impl;
} // namespace storage

namespace storage::local_compaction {

class compaction_sink;
struct cleaned_range;

/// Source implementation for local storage compaction. Reads local segments
/// via log readers using a two-pass algorithm: a reverse pass to build the
/// key-offset map over dirty ranges, then a forward deduplication pass that
/// iterates per-segment and feeds records through the compaction filter.
class compaction_source final
  : public compaction::sliding_window_reducer::source {
public:
    compaction_source(
      disk_log_impl& log,
      model_offset_interval_set dirty_ranges,
      model_offset_interval_set removable_tombstone_ranges,
      const compaction::compaction_config& cfg);

    ss::future<> initialize() final;
    ss::future<ss::stop_iteration> map_building_iteration() final;
    ss::future<ss::stop_iteration>
    deduplication_iteration(compaction::sliding_window_reducer::sink&) final;

    /// Cleaned ranges accumulated during map building. The worker moves
    /// these into the sink after the map building phase completes.
    chunked_vector<cleaned_range>& new_cleaned_ranges() {
        return _new_cleaned_ranges;
    }

private:
    bool preempted() const;

    disk_log_impl& _log;
    model_offset_interval_set _dirty_ranges;
    model_offset_interval_set _removable_tombstone_ranges;
    const compaction::compaction_config& _cfg;

    using interval = model_offset_interval_set::interval;
    using interval_vec = chunked_vector<interval>;

    /// Dirty range intervals materialized from _dirty_ranges during
    /// initialize(), iterated in reverse during map building.
    interval_vec _dirty_range_intervals;
    interval_vec::const_reverse_iterator _dirty_range_it;

    /// Forward segment iterator index used during deduplication.
    size_t _dedup_seg_idx{0};

    /// Cleaned ranges accumulated during the map building pass.
    chunked_vector<cleaned_range> _new_cleaned_ranges;
};

} // namespace storage::local_compaction
