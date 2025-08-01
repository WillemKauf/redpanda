// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "compaction/fwd.h"
#include "compaction/reducer.h"
#include "compaction/types.h"
#include "container/chunked_hash_map.h"
#include "container/fragmented_vector.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "storage/disk_log_impl.h"
#include "storage/exceptions.h"
#include "storage/fs_utils.h"
#include "storage/scoped_file_tracker.h"
#include "storage/segment_set.h"
#include "storage/types.h"

namespace storage {

class storage_compaction_source final : public compaction::reducer::source {
public:
    storage_compaction_source(
      disk_log_impl* log,
      std::optional<model::offset> new_start_offset,
      const compaction::compaction_config& cfg);
    ss::future<> initialize() final;
    bool is_end_of_stream() const final;
    ss::future<bool> end_of_stream() const final;
    ss::future<ss::stop_iteration> backward_pass_iteration() final;
    ss::future<ss::stop_iteration>
    forward_pass_iteration(compaction::reducer::sink&) final;

private:
    using storage_t = segment_set;

    // The `log` which provides a source of `segment`s for this compaction
    // operation.
    disk_log_impl* _log;

    // The start offset of the log after scheduled garbage collection of the
    // `log` finishes. It is expected that garbage collection is invoked
    // immediately before this source is created, and it would therefore be a
    // waste of resources to compact away data below this offset.
    std::optional<model::offset> _new_start_offset;

    const compaction::compaction_config& _cfg;

    // The `_log`'s probe.
    probe& _probe;

    // The set of `segment`s for this compaction operation. Guaranteed to have a
    // value after calling initalize().
    std::optional<storage_t> _segs;

    // Reverse iterator over _segs for backward pass. Guaranteed to have a value
    // after calling initalize().
    storage_t::reverse_iterator _b_it;

    // Iterator over _segs for forward pass. Guaranteed to have a value after
    // calling initalize().
    storage_t::iterator _f_it;

    // Minimum offset fully indexed in the backwards pass. This is aligned to
    // the base offset of the last fully indexed `segment`. If empty (i.e
    // `std::nullopt`), no `segment` could be fully indexed during the backwards
    // pass.
    std::optional<model::offset> _min_offset_fully_indexed;

    // Maintains a counter of accumulated `segment`s per Raft term, in helping
    // to decide whether compaction should occur for a given `segment` (i.e we
    // shouldn't rewrite single `segment`s if no data is to be removed and there
    // aren't any other `segment`s in that term to concatenate with).
    // TODO: get rid of this once raft term is decoupled from segment.
    mutable chunked_hash_map<model::term_id, size_t> _segments_per_term;
};

class storage_compaction_sink final : public compaction::reducer::sink {
public:
    storage_compaction_sink(
      disk_log_impl* log, const compaction::compaction_config& cfg);

    ss::future<ss::stop_iteration>
    operator()(model::record_batch b, model::compression c) final;
    // Initializes the `_appender`, `_idx`, and `_compacted_idx` writers.
    // Unfortunately this (currently) requires knowledge of the underlying
    // storage type due to limitations of local storage, namely:
    // 1. We cannot merge adjacent `segment`s with differing Raft terms.
    // 2. We cannot index an offset space that exceeds the limits of `uint32_t`
    // within a single `segment` (due to types used within the `segment_index`
    // and the potential for overflow).
    // 3. It may be desirable to maintain the compaction status-quo of
    // preserving `segment` start offsets during compaction.
    ss::future<> maybe_initialize(
      ss::lw_shared_ptr<segment> seg,
      std::optional<model::offset> min_offset_fully_indexed);

    ss::future<> finalize() final;

private:
    // Initializes writers and other state using `seg`'s base offset and raft
    // term.
    ss::future<> initialize(
      ss::lw_shared_ptr<segment> seg,
      std::optional<model::offset> min_offset_fully_indexed);

    // Rolls existing writers & resets them to `nullptr`, with other state
    // reset/cleared as well. A call to `initialize()` should follow if more
    // data is to be written in this round of compaction.
    ss::future<> roll();

    // Indexes records within batch `b` in appropriate writers, and then
    // re-compresses with compression type `c` before appending to the currently
    // in-progress `segment`.
    ss::future<> write_batch(model::record_batch b, model::compression c);

    // The `log` which provided a source of `segment`s for this compaction
    // operation.
    disk_log_impl* _log;

    // The compaction config.
    const compaction::compaction_config& _cfg;

    // The `_log`'s probe.
    probe& _probe;

    // In-progress `segment` writers. Guaranteed to have a value after
    // `maybe_initialize()` is called.
    std::unique_ptr<segment_appender> _appender;
    std::unique_ptr<index_state> _idx;
    std::unique_ptr<compacted_index_writer> _compacted_idx;

    // The minimum offset fully indexed during the forward pass, mapped to a
    // `segment`'s base offset boundary.
    model::offset _min_offset_fully_indexed{model::offset::max()};

    // The temporary path for the currently in-progress `segment`.
    std::optional<segment_full_path> _tmpname;

    // The `segment` which will be replaced by the currently in-progress
    // `segment` produced by the writers above.
    ss::lw_shared_ptr<segment> _replace_segment;

    // The container of `segment`s accumulated in the currently in-progress
    // `segment`. It would be a lot nicer if we didn't have this here.
    chunked_vector<ss::lw_shared_ptr<segment>> _accumulated_segments;

    // The container of `generation_id`s for the `segment`s pre-compaction.
    // We must check these _before_ issuing rewrites over the accumulated
    // `segment`s, _after_ obtaining the appropriate locks to ensure we do not
    // race with e.g. a truncation or other `segment` mutation.
    chunked_vector<segment::generation_id> _generations;

    struct {
        void reset() {
            removed_dirty_bytes = 0;
            dirty_turning_clean_bytes = 0;
            total_bytes = 0;
            prev_appender_size = 0;
            index_acc = 0;
        }
        // The number of bytes from dirty `segment`s accumulated in the
        // currently in-progress `segment` which will be removed by the
        // compaction process.
        size_t removed_dirty_bytes{0};
        // The number of bytes from dirty `segment`s accumulated in the
        // currently in-progress `segment` which will be marked clean by the
        // compaction process.
        size_t dirty_turning_clean_bytes{0};
        // The total number of bytes from `segment`s accumulated in the
        // currently in-progress `segment`.
        size_t total_bytes{0};
        // The `file_byte_offset()` (i.e physical size) of the appender which is
        // set on appender roll or upon arrival of a new segment. The difference
        // between `file_byte_offset()` and `prev_appender_size` should, at that
        // moment, tell us how many bytes were preserved from a given `segment`
        // during compaction.
        size_t prev_appender_size{0};
        // Accumulator used for book keeping entries within the `segment_index`
        // being written.
        size_t index_acc{0};
    } _acc;

    // Scoped file tracker for temporary files that may require clean-up in case
    // of an incomplete compaction run.
    std::optional<scoped_file_tracker> _tmp_file_tracker;
};

} // namespace storage
