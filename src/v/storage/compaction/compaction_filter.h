// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "compaction/filter.h"
#include "compaction/key_offset_map.h"
#include "storage/compaction/compaction_state.h"

#include <seastar/core/future.hh>

#include <optional>
#include <vector>

namespace storage::local_compaction {

class compaction_filter final : public compaction::filter {
public:
    compaction_filter(
      compaction::sliding_window_reducer::sink& sink,
      const compaction::key_offset_map& map,
      model::ntp ntp,
      const model_offset_interval_set& removable_tombstone_ranges);

private:
    ss::future<bool>
    should_keep(const model::record_batch&, const model::record&) const;

    ss::future<std::vector<int32_t>>
    compute_offset_deltas_to_keep(const model::record_batch& b) const final;

    ss::future<std::optional<model::record_batch>>
    filter_batch_with_offset_deltas(
      model::record_batch b, std::vector<int32_t> offset_deltas) const final;

    const compaction::key_offset_map& _map;
    const model_offset_interval_set& _removable_tombstone_ranges;
};

} // namespace storage::local_compaction
