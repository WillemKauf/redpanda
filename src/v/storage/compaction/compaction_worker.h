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

#include <filesystem>
#include <memory>

namespace compaction {
class hash_key_offset_map;
} // namespace compaction

namespace storage {
class disk_log_impl;
} // namespace storage

namespace storage::local_compaction {

/// \brief Orchestrates one round of key-deduplication compaction on a
/// local log segment range. Owns the key_offset_map, computes dirty and
/// tombstone-removable ranges from compaction_state, drives the
/// sliding_window_reducer, and persists the updated state to disk.
class compaction_worker {
public:
    compaction_worker();

    /// Run one round of compaction on the given log. Reads dirty ranges
    /// from state, drives the source/sink pipeline, and persists the
    /// updated compaction_state to <partition_dir>/compaction_state.
    ss::future<> compact(
      disk_log_impl& log,
      compaction_state& state,
      const compaction::compaction_config& cfg);

private:
    /// Lazily allocates the hash_key_offset_map using the compaction
    /// memory reservation from memory_groups().
    ss::future<> initialize_map();

    /// Writes state to <partition_dir>/compaction_state atomically via a
    /// write-to-tmp-then-rename pattern.
    ss::future<> persist_compaction_state(
      const std::filesystem::path& partition_dir,
      const compaction_state& state);

    std::unique_ptr<compaction::hash_key_offset_map> _map;
};

} // namespace storage::local_compaction
