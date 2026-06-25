/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "dedup/produce_filter.h"

#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "container/chunked_vector.h"
#include "model/record_utils.h"

#include <seastar/core/coroutine.hh>

#include <optional>
#include <vector>

namespace dedup {

namespace {

std::optional<bytes> find_dedup_id(const model::record& rec) {
    for (const auto& h : rec.headers()) {
        if (h.key() == dedup_id_header_key) {
            return iobuf_to_bytes(h.value());
        }
    }
    return std::nullopt;
}

struct ided_record {
    int32_t index;
    bytes id;
    model::timestamp ts;
};

} // namespace

ss::future<filter_result>
dedup_filter_batch(model::record_batch batch, const lazy_map& get_map) {
    const auto& hdr = batch.header();

    // Batches whose framing must not be rewritten. Idempotent/transactional
    // producers rely on exact record layout and are deduplicated separately by
    // rm_stm; control batches carry no user records.
    const auto bid = model::batch_identity::from(hdr);
    if (bid.is_idempotent() || bid.is_transactional || hdr.attrs.is_control()) {
        co_return filter_result{
          .outcome = filter_outcome::unchanged, .batch = std::move(batch)};
    }

    // Pass 1: collect the records that carry a dedup-id header. Records without
    // one are never deduplicated. This pass allocates nothing in the map and
    // does not touch `get_map`.
    const auto base_ts = hdr.first_timestamp;
    chunked_vector<ided_record> ided;
    int32_t idx = 0;
    batch.for_each_record([&ided, &idx, base_ts](const model::record& rec) {
        if (auto id = find_dedup_id(rec); id.has_value()) {
            ided.push_back(ided_record{
              .index = idx,
              .id = std::move(*id),
              .ts = model::timestamp(base_ts() + rec.timestamp_delta())});
        }
        ++idx;
    });

    // No dedup-id records: nothing to do, and crucially the map is never
    // created for this partition until a dedup-id record actually arrives.
    if (ided.empty()) {
        co_return filter_result{
          .outcome = filter_outcome::unchanged, .batch = std::move(batch)};
    }

    // Pass 2: query the dedup window. get_map() lazily allocates here.
    auto& map = get_map();
    std::vector<bool> dropped(hdr.record_count, false);
    int32_t drop_count = 0;
    for (const auto& e : ided) {
        auto d = co_await map.check_and_record(e.id, e.ts);
        if (d == decision::drop) {
            dropped[e.index] = true;
            ++drop_count;
        }
    }

    if (drop_count == 0) {
        co_return filter_result{
          .outcome = filter_outcome::unchanged, .batch = std::move(batch)};
    }

    const auto total = hdr.record_count;
    if (drop_count == total) {
        co_return filter_result{
          .outcome = filter_outcome::fully_duplicate,
          .batch = std::move(batch),
          .records_dropped = total};
    }

    // Pass 3: re-encode the surviving records, preserving their original
    // offset_deltas (and the batch's last_offset_delta), leaving offset holes
    // for the dropped records.
    iobuf kept_records;
    int32_t kept = 0;
    int32_t i = 0;
    batch.for_each_record(
      [&kept_records, &kept, &dropped, &i](const model::record& rec) {
          if (!dropped[i++]) {
              model::append_record_to_buffer(kept_records, rec);
              ++kept;
          }
      });

    auto new_hdr = hdr;
    new_hdr.record_count = kept;
    new_hdr.reset_size_checksum_metadata(kept_records);
    co_return filter_result{
      .outcome = filter_outcome::rewritten,
      .batch = model::record_batch(
        new_hdr, std::move(kept_records), model::record_batch::tag_ctor_ng{}),
      .records_dropped = total - kept};
}

} // namespace dedup
