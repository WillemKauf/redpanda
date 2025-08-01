// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "filter.h"

#include "compaction/logger.h"
#include "compaction/utils.h"
#include "model/record.h"
#include "storage/parser_utils.h"

#include <vector>

namespace compaction {

ss::future<ss::stop_iteration> filter::operator()(model::record_batch b) {
    const auto comp = b.header().attrs.compression();
    if (!b.compressed()) {
        co_return co_await filter_and_rewrite_with_sink(comp, std::move(b));
    }
    auto batch = co_await storage::internal::decompress_batch(std::move(b));

    co_return co_await filter_and_rewrite_with_sink(comp, std::move(batch));
}

ss::future<> filter::maybe_keep_offset(
  const model::record_batch& batch,
  const model::record& r,
  bool is_last_record_in_batch,
  std::vector<int32_t>& offset_deltas) const {
    if (co_await _should_keep_fn(batch, r, is_last_record_in_batch)) {
        offset_deltas.push_back(r.offset_delta());
    }

    co_return;
}

ss::future<std::optional<model::record_batch>>
filter::filter_batch(model::record_batch b) const {
    // do not filter non-removable batch types under any circumstances
    if (!is_filterable(b.header().type)) {
        co_return std::move(b);
    }

    // 1. compute which records to keep
    std::vector<int32_t> offset_deltas;
    offset_deltas.reserve(b.record_count());

    int32_t records_seen = 0;
    co_await b.for_each_record_async(
      [this, &b, &offset_deltas, &records_seen](const model::record& r) {
          ++records_seen;
          return maybe_keep_offset(
            b, r, b.record_count() == records_seen, offset_deltas);
      });

    if (
      _compaction_placeholder_enabled && b.last_offset() == _last_offset
      && offset_deltas.empty()) {
        // last batch in the object has been compacted away. We install a
        // placeholder batch of same size to retain contiguousness of the offset
        // space.
        auto placeholder = make_placeholder_batch(b.header());
        vlog(
          compactlog.debug,
          "installing a placeholder {} for compacted batch: {}",
          placeholder,
          b);
        co_return placeholder;
    }

    // 2. no record to keep
    if (offset_deltas.empty()) {
        co_return std::nullopt;
    }

    // 3. keep all records
    if (offset_deltas.size() == static_cast<size_t>(b.record_count())) {
        co_return std::move(b);
    }

    // 4. filter
    iobuf ret;
    int32_t rec_count = 0;
    std::optional<int64_t> first_timestamp_delta;
    int64_t last_timestamp_delta;
    b.for_each_record([&rec_count,
                       &first_timestamp_delta,
                       &last_timestamp_delta,
                       &ret,
                       &offset_deltas](model::record record) {
        // contains the key
        if (std::count(
              offset_deltas.begin(),
              offset_deltas.end(),
              record.offset_delta())) {
            /*
             * TODO when we further optimize lazy record materialization ot
             * make use of views we can avoid this re-encoding by copying or
             * sharing the view. either way, we were building
             * record batch with the uncompressed records so they were being
             * re-encoded.
             */
            if (!first_timestamp_delta) {
                first_timestamp_delta = record.timestamp_delta();
            }
            last_timestamp_delta = record.timestamp_delta();
            model::append_record_to_buffer(ret, record);
            ++rec_count;
        }
    });

    if (rec_count == 0) {
        co_return std::nullopt;
    }

    // There is no need to preserve the timestamp from the original
    // batch after compaction. The FirstTimestamp field therefore always
    // reflects the timestamp of the first record in the batch. If the batch is
    // empty, the FirstTimestamp will be set to -1 (NO_TIMESTAMP).
    //
    // Similarly, the MaxTimestamp field reflects the maximum timestamp of the
    // current records if the timestamp type is CREATE_TIME. For
    // LOG_APPEND_TIME, on the other hand, the MaxTimestamp field reflects the
    // timestamp set by the broker and is preserved after compaction.
    // Additionally, the MaxTimestamp of an empty batch always retains the
    // previous value prior to becoming empty.
    auto& hdr = b.header();
    const auto first_time = model::timestamp(
      hdr.first_timestamp() + first_timestamp_delta.value());
    auto last_time = hdr.max_timestamp;
    if (hdr.attrs.timestamp_type() == model::timestamp_type::create_time) {
        last_time = model::timestamp(first_time() + last_timestamp_delta);
    }
    auto new_hdr = hdr;
    new_hdr.first_timestamp = first_time;
    new_hdr.max_timestamp = last_time;
    new_hdr.record_count = rec_count;
    storage::internal::reset_size_checksum_metadata(new_hdr, ret);
    auto new_batch = model::record_batch(
      new_hdr, std::move(ret), model::record_batch::tag_ctor_ng{});
    co_return new_batch;
}

ss::future<ss::stop_iteration> filter::filter_and_rewrite_with_sink(
  model::compression original, model::record_batch b) {
    ++_stats.batches_processed;
    const auto record_count_before = b.record_count();
    auto to_copy = co_await filter_batch(std::move(b));
    if (to_copy.has_value()) {
        const auto records_to_remove = record_count_before
                                       - to_copy->record_count();
        _stats.records_discarded += records_to_remove;
        bool compactible_batch = is_compactible(_ntp, to_copy->header());
        if (!compactible_batch) {
            ++_stats.non_compactible_batches;
        }

        co_await _sink(std::move(to_copy).value(), original);
    } else {
        ++_stats.batches_discarded;
        _stats.records_discarded += record_count_before;
    }

    co_return ss::stop_iteration::no;
}

} // namespace compaction
