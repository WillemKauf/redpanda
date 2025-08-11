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

template<typename Sink_T>
class storage_compaction_source final : public compaction::reducer::source {
public:
    storage_compaction_source(
      disk_log_impl* log,
      std::optional<model::offset> new_start_offset,
      const compaction::compaction_config& cfg);
    ss::future<> initialize_source() final;
    ss::future<> initialize_sink(compaction::reducer::sink&) final;
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
    // value after calling initialize().
    std::optional<storage_t> _segs;

    // Reverse iterator over _segs for backward pass. Guaranteed to have a value
    // after calling initialize().
    storage_t::reverse_iterator _b_it;

    // Iterator over _segs for forward pass. Guaranteed to have a value after
    // calling initialize().
    storage_t::iterator _f_it;

    // Minimum offset fully indexed in the backwards pass. This is aligned to
    // the base offset of the last fully indexed `segment`. If empty (i.e
    // `std::nullopt`), no `segment` could be fully indexed during the backwards
    // pass.
    std::optional<model::offset> _min_offset_fully_indexed;
};

class storage_compaction_sink final : public compaction::reducer::sink {
public:
    storage_compaction_sink(
      disk_log_impl* log, const compaction::compaction_config& cfg);

    ss::future<ss::stop_iteration>
    operator()(model::record_batch b, model::compression c) final;

    ss::future<> filter_segments(segment_set& src_segs);

    ss::future<> finalize() final;

    ss::future<std::optional<ss::lw_shared_ptr<segment>>>
    maybe_roll(ss::lw_shared_ptr<segment> seg);

private:
    // Initializes the `_appender`, `_idx`, and `_c{ompacted}idx` writers.
    // Unfortunately this (currently) requires knowledge of the underlying
    // storage type due to limitations of local storage, namely:
    // 1. We cannot merge adjacent `segment`s with differing Raft terms.
    // 2. We cannot index an offset space that exceeds the limits of `uint32_t`
    // within a single `segment` (due to types used within the `segment_index`
    // and the potential for overflow).
    // 3. It may be desirable to maintain the compaction status-quo of
    // preserving `segment` start offsets during compaction.
    ss::future<> initialize_writers(ss::lw_shared_ptr<segment> seg);

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

    // In-progress `segment`. Guaranteed to have a value after
    // `maybe_initialize()` is called.
    ss::lw_shared_ptr<segment> _segment;

    // The minimum offset fully indexed during the forward pass, mapped to a
    // `segment`'s base offset boundary.
    model::offset _min_offset_fully_indexed{model::offset::max()};

    // The temporary path for the currently in-progress `segment`.
    std::optional<segment_full_path> _tmpname;

    struct {
        void reset() {
            removed_dirty_bytes = 0;
            dirty_turning_clean_bytes = 0;
            total_bytes = 0;
            prev_appender_size = 0;
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
    } _acc;

    // Scoped file tracker for temporary files that may require clean-up in case
    // of an incomplete compaction run.
    std::optional<scoped_file_tracker> _tmp_file_tracker;
};

} // namespace storage
