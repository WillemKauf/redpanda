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

#include "compaction/types.h"
#include "storage/compaction/compaction_state.h"

#include <seastar/core/future.hh>

namespace storage {
class disk_log_impl;
} // namespace storage

namespace storage::local_compaction {

/// \brief Orchestrates one round of key-deduplication compaction on a
/// local log segment range. Computes dirty and tombstone-removable ranges
/// from compaction_state, drives the sliding_window_reducer, and the sink
/// persists the updated state to the log's kvstore.
class compaction_worker {
public:
    /// Run one round of compaction on the given log.
    /// cfg.hash_key_map must be non-null and already initialized.
    ss::future<> compact(
      disk_log_impl& log,
      compaction_state& state,
      const compaction::compaction_config& cfg);
};

} // namespace storage::local_compaction
