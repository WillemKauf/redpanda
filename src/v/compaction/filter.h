// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "compaction/reducer.h"
#include "compaction/types.h"
#include "model/record.h"

namespace compaction {

// Wrapper around a `sink&` for use with `record_batch_reader` interface.
// TODO: Better encapsulation of key_offset_map within this class (since that is
// part of the generic "compaction" algorithm) instead of deferring to
// implementation specific `filter_t`.
class filter {
public:
    using filter_t = ss::noncopyable_function<ss::future<bool>(
      const model::record_batch&, const model::record&, bool)>;

    filter(
      filter_t f,
      reducer::sink& sink,
      bool compaction_placeholder_enabled,
      model::offset last_offset,
      model::ntp ntp)
      : _should_keep_fn(std::move(f))
      , _sink(sink)
      , _compaction_placeholder_enabled(compaction_placeholder_enabled)
      , _last_offset(last_offset)
      , _ntp(std::move(ntp)) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch b);
    stats end_of_stream() const { return _stats; }

private:
    // Uses the contained `_should_keep_fn` to determine if the provided record
    // should be preserved when filtering batches.
    ss::future<> maybe_keep_offset(
      const model::record_batch& batch,
      const model::record& r,
      bool is_last_record_in_batch,
      std::vector<int32_t>& offset_deltas) const;

    // Iterates over records in the provided batch, filtering out records per
    // the result of `maybe_keep_offset()`.
    ss::future<std::optional<model::record_batch>>
    filter_batch(model::record_batch b) const;

    // Performs filtering over the entire batch, and then delegates the result
    // to `_sink` for writing.
    ss::future<ss::stop_iteration> filter_and_rewrite_with_sink(
      model::compression original, model::record_batch b);

    // Passed state
    filter_t _should_keep_fn;
    reducer::sink& _sink;
    bool _compaction_placeholder_enabled;
    model::offset _last_offset;
    model::ntp _ntp;

    // Managed state
    stats _stats;
};

} // namespace compaction
