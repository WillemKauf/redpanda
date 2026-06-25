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

#pragma once

#include "base/seastarx.h"
#include "dedup/windowed_dedup_map.h"
#include "model/record.h"

#include <seastar/core/future.hh>
#include <seastar/util/noncopyable_function.hh>

#include <cstdint>
#include <string_view>

namespace dedup {

/// Record header key that carries the deduplication identifier. Only records
/// that set this header are eligible for produce-path deduplication; the
/// record key is never used (it routes/compacts and must not be overloaded).
inline constexpr std::string_view dedup_id_header_key = "redpanda.dedup.id";

enum class filter_outcome : uint8_t {
    /// No records were dropped; the batch is returned unmodified.
    unchanged,
    /// One or more records were dropped and the batch was rebuilt.
    rewritten,
    /// Every record was a duplicate; the batch should not be replicated.
    fully_duplicate,
};

struct filter_result {
    filter_outcome outcome;
    /// The batch to replicate. Valid for `unchanged` and `rewritten`; for
    /// `fully_duplicate` there is nothing to replicate and this holds the
    /// now-empty original batch.
    model::record_batch batch;
    int32_t records_dropped{0};
};

/// Lazily obtain the dedup map, creating it on first call. It is invoked only
/// when the batch actually contains a record carrying the dedup-id header, so
/// a partition that has the dedup property set but never receives a dedup-id
/// record allocates nothing.
using lazy_map = ss::noncopyable_function<windowed_dedup_map&()>;

/// Run window-based deduplication over the records of a produce batch.
///
/// Only records carrying the `redpanda.dedup.id` header are considered; their
/// header value is the dedup key. Records without it are always kept.
/// Idempotent, transactional, and control batches are returned `unchanged`
/// (their framing must not be altered).
///
/// Surviving records keep their original `offset_delta`s and the batch keeps
/// its `last_offset_delta`, so the batch still spans its original offset range
/// (dropped records leave offset holes, as log compaction does).
ss::future<filter_result>
dedup_filter_batch(model::record_batch batch, const lazy_map& get_map);

} // namespace dedup
