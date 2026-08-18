/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "bytes/iobuf.h"
#include "model/record.h"
#include "storage/version.h"

#include <span>

namespace storage {

/// Size of the on-disk batch header of version v1 segments.
inline constexpr size_t v1_record_batch_header_size
  = model::packed_record_batch_header_size;
static_assert(v1_record_batch_header_size == 61);

// Serializes the header to a buffer suitable to be stored on disk in version
// v1 segments. Note that this is different from serde::envelope serialization
// in that only the exact batch header fields are serialized, with no
// additional bytes for size, versions, etc.
iobuf batch_header_to_disk_iobuf(const model::record_batch_header& h);
model::record_batch_header batch_header_from_disk_iobuf(iobuf b);
model::record_batch_header
batch_header_from_disk_buf(std::span<const char> data);

/// Size of the on-disk batch header of version v2 segments (includes the raft
/// term in addition to everything in a version v1 segment)
inline constexpr size_t v2_record_batch_header_size
  = model::packed_record_batch_header_size
    + sizeof(model::record_batch_header::context::term);
static_assert(v2_record_batch_header_size == 69);

// Serializes the header to a buffer suitable to be stored on disk in version v2
// segments.
iobuf v2_batch_header_to_disk_iobuf(const model::record_batch_header& h);
model::record_batch_header v2_batch_header_from_disk_iobuf(iobuf b);
model::record_batch_header
v2_batch_header_from_disk_buf(std::span<const char> data);

// Returns the total on disk size of the provided record batch (e.g. the sum
// of the size of records and of the header itself).
size_t batch_on_disk_size(
  const model::record_batch_header& h, record_version_type version);

} // namespace storage
