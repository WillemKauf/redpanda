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

#include "compaction/reducer.h"
#include "container/offset_interval_set.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "storage/compaction/compaction_state.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

namespace storage {
class disk_log_impl;
class segment;
} // namespace storage

namespace storage::local_compaction {

/// A range of offsets that was cleaned by this compaction pass. The worker
/// uses these to update the persistent compaction_state.
struct cleaned_range {
    model::offset base_offset;
    model::offset last_offset;
    bool has_tombstones;
};

/// Sink implementation for local storage compaction. Receives deduplicated
/// batches from the compaction filter and writes them to new local segments,
/// rolling segments on term changes, size limits, or offset span overflow.
/// After writing, calls disk_log_impl::replace_offset_range() to atomically
/// swap old segments with new ones.
class compaction_sink final
  : public compaction::sliding_window_reducer::sink {
public:
    compaction_sink(
      disk_log_impl& log,
      model_offset_interval_set removable_tombstone_ranges,
      size_t max_compacted_segment_size,
      ss::abort_source& as);

    ss::future<bool>
    initialize(compaction::sliding_window_reducer::source&) final;

    ss::future<> prepare_iteration(kafka::offset) final;

    ss::future<ss::stop_iteration>
    operator()(model::record_batch, model::compression) final;

    ss::future<> finish_iteration(kafka::offset, kafka::offset) final;

    ss::future<> finalize(bool success) final;

    /// Called by the source before prepare_iteration to set the raft term
    /// for the current input segment.
    void set_current_term(model::term_id t) { _current_term = t; }

    /// Results available after finalize(true).
    const chunked_vector<cleaned_range>& new_cleaned_ranges() const {
        return _new_cleaned_ranges;
    }
    const model_offset_interval_set& processed_ranges() const {
        return _processed_ranges;
    }
    const model_offset_interval_set& removable_tombstone_ranges() const {
        return _removable_tombstone_ranges;
    }

private:
    /// Create a new staging segment and begin appending.
    ss::future<> open_staging_segment(model::offset base, model::term_id term);

    /// Flush the current staging segment and replace the corresponding
    /// range in the log.
    ss::future<> roll();

    /// Close and remove the in-flight staging segment without committing.
    ss::future<> discard_inflight();

    disk_log_impl& _log;
    model_offset_interval_set _removable_tombstone_ranges;
    size_t _max_compacted_segment_size;
    ss::abort_source& _as;

    /// The current raft term, set by the source before each iteration.
    model::term_id _current_term;

    /// The term used when the current staging segment was opened.
    model::term_id _staging_term;

    /// The in-flight staging segment being written to.
    ss::lw_shared_ptr<segment> _staging_segment;

    /// The model::offset of the first batch appended to the current range
    /// being replaced (may span multiple rolls if the range is large).
    model::offset _range_start;

    /// The model::offset of the last batch appended to the current staging
    /// segment.
    model::offset _range_end;

    /// Cleaned ranges accumulated from the source during initialize().
    chunked_vector<cleaned_range> _new_cleaned_ranges;

    /// Offset ranges that have been fully processed (written and committed).
    model_offset_interval_set _processed_ranges;
};

} // namespace storage::local_compaction
