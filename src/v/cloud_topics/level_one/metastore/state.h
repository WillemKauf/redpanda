/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "absl/container/btree_set.h"
#include "base/seastarx.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/offset_interval_set.h"
#include "compaction/compaction_state.h"
#include "container/chunked_hash_map.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "serde/envelope.h"
#include "serde/rw/envelope.h"
#include "serde/rw/optional.h"
#include "serde/rw/set.h"

#include <seastar/core/future.hh>

#include <expected>

namespace cloud_topics::l1 {

// Represents the state managed by the replicated state machine that serves L1
// metadata.

// A description of a portion of an object that points at data for a contiguous
// range of batches belonging to a single partition. The object itself may
// contain ranges of data from many partitions.
struct extent
  : public serde::
      envelope<extent, serde::version<0>, serde::compat_version<0>> {
    friend bool operator==(const extent&, const extent&) = default;
    std::strong_ordering operator<=>(const extent&) const = default;
    auto serde_fields() {
        return std::tie(
          base_offset, last_offset, max_timestamp, filepos, len, oid);
    }

    kafka::offset base_offset;
    kafka::offset last_offset;
    model::timestamp max_timestamp;
    size_t filepos;
    size_t len;
    // TODO: avoid duplicating the UUIDs with some indirection.
    object_id oid;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it,
          "{{offsets:({}~{}), max_timestamp:{}, filepos:{}, len:{}, oid:{}",
          base_offset,
          last_offset,
          max_timestamp,
          filepos,
          len,
          oid);
    }
};

// Offset that corresponds to the start of a given term.
struct term_start
  : public serde::
      envelope<term_start, serde::version<0>, serde::compat_version<0>> {
    friend bool operator==(const term_start&, const term_start&) = default;
    auto serde_fields() { return std::tie(term_id, start_offset); }
    auto operator<=>(const term_start&) const = default;

    model::term_id term_id;
    kafka::offset start_offset;
};

using compaction_state = compaction::compaction_state<kafka::offset>;

// State tracked per Kafka partition. The extents added to this state must have
// no overlaps and no gaps in order to ensure there is no data loss.
struct partition_state
  : public serde::
      envelope<partition_state, serde::version<0>, serde::compat_version<0>> {
    friend bool operator==(const partition_state&, const partition_state&)
      = default;
    auto serde_fields() {
        return std::tie(
          extents,
          start_offset,
          next_offset,
          compaction_state,
          compaction_epoch,
          term_starts);
    }

    partition_state copy() const;

    // The list of extents that comprise this partition. The ordering here
    // allows us to perform efficient lookups by offset.
    // Using a set here allows us to:
    // - perform efficient lookups by offset
    // - remove elements from the beginning e.g. for prefix truncation
    // - replace ranges in the middle with new extents e.g. for compaction
    using extent_set_t = absl::btree_set<extent>;
    extent_set_t extents;

    // Brute force calculation of the byte size of all the extents for this
    // partition. This is used for the simple metastore which isn't used in
    // production, so efficiency isn't critical.
    size_t calculate_size() const {
        size_t total = 0;
        for (const auto& ext : extents) {
            total += ext.len;
        }
        return total;
    }

    // The start offset of the partition. This may not align with the front of
    // `extents` if the offset was set through the Kafka API.
    kafka::offset start_offset{0};

    // The next offset expected to be added to this partition. In general this
    // should align with the back of `extents`, but is required when `extents`
    // is empty.
    kafka::offset next_offset{0};

    // Describes the current state of compacted regions of this partition's
    // log, if it belongs to a compacted topic.
    //
    // Empty iff compaction has not been run for this partition.
    // TODO: should we remove this if cleanup policy is switched?
    std::optional<compaction_state> compaction_state;

    using compaction_epoch_t
      = named_type<int64_t, struct state_compaction_epoch>;
    // The current epoch of this partition's compacted log. Effectively
    // describes the number of logical changes made to the log's underlying
    // data. Incremented when a compaction or partial compaction has been
    // applied to this partition.
    // This is a useful field for optimistic concurrency control, preventing a
    // compaction job based on stale data from overwriting an updated log.
    compaction_epoch_t compaction_epoch{0};

    // A mapping of terms (used instead of Kafka epochs) to the starting Kafka
    // offset for that term. Both the term and offsets are maintained to be
    // monotonically strictly increasing.
    //
    // TODO: when we implement set_start_offset(), we should retain enough
    // information to return a value for the start_offset, even when the log
    // has been prefix truncated to be empty. I.e. this list should never be
    // empty once there has been data in the log.
    absl::btree_set<term_start> term_starts;
};

// Tracks the state managed for each partition of a Kafka topic.
struct topic_state
  : public serde::
      envelope<topic_state, serde::version<0>, serde::compat_version<0>> {
    friend bool operator==(const topic_state&, const topic_state&) = default;
    auto serde_fields() { return std::tie(pid_to_state); }
    chunked_hash_map<model::partition_id, partition_state> pid_to_state;

    topic_state copy() const;
};

// Metadata about a given object that is not specific to any partition.
struct object_entry
  : public serde::
      envelope<object_entry, serde::version<1>, serde::compat_version<0>> {
    friend bool operator==(const object_entry&, const object_entry&) = default;
    auto serde_fields() {
        return std::tie(
          total_data_size,
          removed_data_size,
          footer_pos,
          object_size,
          last_updated,
          is_preregistration);
    }
    size_t total_data_size{0};
    size_t removed_data_size{0};
    size_t footer_pos{0};
    size_t object_size{0};
    model::timestamp last_updated;
    bool is_preregistration{false};
};

// Tracks the state of each topic revision.
struct state
  : public serde::envelope<state, serde::version<0>, serde::compat_version<0>> {
    friend bool operator==(const state&, const state&) = default;
    auto serde_fields() { return std::tie(topic_to_state, objects); }
    chunked_hash_map<model::topic_id, topic_state> topic_to_state;
    chunked_hash_map<object_id, object_entry> objects;

    state copy() const;

    std::optional<std::reference_wrapper<const partition_state>>
    partition_state(const model::topic_id_partition&) const;
};

struct stm_snapshot
  : public serde::
      envelope<stm_snapshot, serde::version<0>, serde::compat_version<0>> {
    state state;

    auto serde_fields() { return std::tie(state); }
};

} // namespace cloud_topics::l1
