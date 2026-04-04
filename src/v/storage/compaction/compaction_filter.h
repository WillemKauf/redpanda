// Copyright 2026 Redpanda Data, Inc.
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
#include <seastar/core/shared_ptr.hh>

#include <optional>
#include <vector>

namespace storage {
class stm_hookset;
} // namespace storage

namespace storage::local_compaction {

class compaction_filter final : public compaction::filter {
public:
    compaction_filter(
      compaction::sliding_window_reducer::sink& sink,
      const compaction::key_offset_map& map,
      model::ntp ntp,
      model::offset segment_last_offset,
      const model_offset_interval_set& removable_tombstone_ranges,
      const model_offset_interval_set& removable_transaction_ranges,
      ss::lw_shared_ptr<storage::stm_hookset> stm_mgr);

private:
    ss::future<bool> should_keep(
      const model::record_batch&, const model::record&) const override;

    ss::future<std::optional<model::record_batch>>
      filter_batch(model::record_batch) const override;

    const compaction::key_offset_map& _map;
    model::offset _segment_last_offset;
    const model_offset_interval_set& _removable_tombstone_ranges;
    const model_offset_interval_set& _removable_transaction_ranges;
    ss::lw_shared_ptr<storage::stm_hookset> _stm_mgr;
};

} // namespace storage::local_compaction
