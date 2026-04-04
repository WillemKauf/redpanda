// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "absl/container/btree_set.h"
#include "container/offset_interval_set.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "serde/envelope.h"
#include "serde/rw/envelope.h"
#include "serde/rw/optional.h"
#include "serde/rw/set.h"

#include <chrono>
#include <optional>

namespace compaction {

template<typename OffsetT>
using compaction_offset_interval_set = container::offset_interval_set<OffsetT>;

template<typename OffsetT>
struct compaction_offsets {
    compaction_offset_interval_set<OffsetT> dirty_ranges;
    compaction_offset_interval_set<OffsetT> removable_tombstone_ranges;
};

template<typename OffsetT>
struct compaction_state
  : serde::envelope<
      compaction_state<OffsetT>,
      serde::version<0>,
      serde::compat_version<0>> {
    struct cleaned_range_with_tombstones
      : serde::envelope<
          cleaned_range_with_tombstones,
          serde::version<0>,
          serde::compat_version<0>> {
        friend bool operator==(
          const cleaned_range_with_tombstones&,
          const cleaned_range_with_tombstones&)
          = default;
        auto operator<=>(const cleaned_range_with_tombstones&) const = default;
        auto serde_fields() {
            return std::tie(
              base_offset, last_offset, cleaned_with_tombstones_at);
        }

        OffsetT base_offset;
        OffsetT last_offset;

        // Timestamp at which this clean range was generated.
        // This is important to track to be able to schedule tombstone removal
        // some time (delete.retention.ms) after cleaning.
        model::timestamp cleaned_with_tombstones_at;
    };
    using tombstone_range_set_t
      = absl::btree_set<cleaned_range_with_tombstones>;

    friend bool operator==(const compaction_state&, const compaction_state&)
      = default;
    auto serde_fields() {
        return std::tie(cleaned_ranges, cleaned_ranges_with_tombstones);
    }

    compaction_state copy() const;

    // Returns false if the input range overlaps with another existing range
    // with tombstones.
    bool may_add(const cleaned_range_with_tombstones&) const;

    // Adds the input range to the set of cleaned ranges with tombstones.
    bool add(const cleaned_range_with_tombstones&);

    // Returns true if the input inclusive range is fully covered by a set of
    // cleaned ranges with tombstones.
    bool has_contiguous_range_with_tombstones(OffsetT, OffsetT) const;

    // Removes the input inclusive range from the set of cleaned ranges with
    // tombstones. The input range doesn't need to align exactly with any of
    // `cleaned_ranges_with_tombstones`, but it must be fully covered.
    //
    // For example, let's say our cleaned ranges with tombstones were:
    // ┌───────────┬───────────┐
    // │           │           │
    // │10..99     │100..199   │
    // │ts=900     │ts=1000    │
    // └───────────┴───────────┘
    //
    // Even though it doesn't align with the bounds of any range, we could
    // erase [80, 129] because that entire range is covered.
    // ┌─────────┬────┬────────┐
    // │         │    │        │
    // │10..79   │    │130..199│
    // │ts=900   │    │ts=1000 │
    // └─────────┴────┴────────┘
    //
    // We are not able to erase [0, 79], because [0, 9] are not covered.
    bool erase_contiguous_range_with_tombstones(OffsetT, OffsetT);

    // Prefix truncates the cleaned_ranges and cleaned_ranges_with_tombstones
    // such that all ranges below the new start are removed and any range that
    // overlaps with the new start is truncated to start at the given offset.
    void truncate_with_new_start_offset(OffsetT);

    // Computes dirty and tombstone-removable ranges.
    //
    // dirty_ranges is the complement of cleaned_ranges within
    // [OffsetT{0}, max_removable_offset].
    //
    // removable_tombstone_ranges contains entries from
    // cleaned_ranges_with_tombstones where enough time has elapsed since
    // cleaning (as determined by tombstone_retention_ms), clamped to
    // max_tombstone_remove_offset.
    compaction_offsets<OffsetT> get_compaction_info(
      OffsetT max_removable_offset,
      OffsetT max_tombstone_remove_offset,
      std::optional<std::chrono::milliseconds> tombstone_retention_ms) const;

    // Ranges of the log whose keys have been deduplicated from the _beginning
    // of the log_ (NOT from the cleaned range's start offset!) to and
    // including the interval's last offset.
    //
    // While extents that overlap with a cleaned range may be replaced when
    // cleaning a dirty range, there is no point in recompacting an offset
    // range that is cleaned (because all the records are already deduplicated)
    // unless it contains tombstones that are eligible for compaction.
    compaction_offset_interval_set<OffsetT> cleaned_ranges;

    // Cleaned offset ranges that contain tombstones, tracked separately to
    // avoid complicating reasoning about cleaned ranges. Ordered, and
    // maintained to be non-overlapping.
    //
    // These must overlap with `cleaned_ranges`.
    //
    // For a tombstone record to be elegible for removal, all offsets at and
    // below it must have been cleaned for at least delete.retention.ms.
    tombstone_range_set_t cleaned_ranges_with_tombstones;

private:
    struct tombstone_range_iters {
        typename tombstone_range_set_t::const_iterator begin;
        typename tombstone_range_set_t::const_iterator last;
    };
    // Returns iterators that span the contiguous, minimal set that fully cover
    // the given inclusive offset range. If no such set of contiguous ranges
    // exist, returns std::nullopt.
    std::optional<tombstone_range_iters>
      get_contiguous_range_with_tombstones(OffsetT, OffsetT) const;
};

template<typename OffsetT>
compaction_state<OffsetT> compaction_state<OffsetT>::copy() const {
    compaction_state res;
    res.cleaned_ranges = cleaned_ranges;
    for (const auto& r : cleaned_ranges_with_tombstones) {
        res.cleaned_ranges_with_tombstones.emplace(r);
    }
    return res;
}

template<typename OffsetT>
bool compaction_state<OffsetT>::has_contiguous_range_with_tombstones(
  OffsetT base_offset, OffsetT last_offset) const {
    return get_contiguous_range_with_tombstones(base_offset, last_offset)
      .has_value();
}

template<typename OffsetT>
bool compaction_state<OffsetT>::erase_contiguous_range_with_tombstones(
  OffsetT base_offset, OffsetT last_offset) {
    auto tombstone_ranges = get_contiguous_range_with_tombstones(
      base_offset, last_offset);
    if (!tombstone_ranges.has_value()) {
        return false;
    }
    std::optional<cleaned_range_with_tombstones> replacement_begin;
    if (tombstone_ranges->begin->base_offset != base_offset) {
        replacement_begin = cleaned_range_with_tombstones{
          .base_offset = tombstone_ranges->begin->base_offset,
          .last_offset = prev_offset(base_offset),
          .cleaned_with_tombstones_at
          = tombstone_ranges->begin->cleaned_with_tombstones_at,
        };
    }
    std::optional<cleaned_range_with_tombstones> replacement_last;
    if (tombstone_ranges->last->last_offset != last_offset) {
        replacement_last = cleaned_range_with_tombstones{
          .base_offset = next_offset(last_offset),
          .last_offset = tombstone_ranges->last->last_offset,
          .cleaned_with_tombstones_at
          = tombstone_ranges->last->cleaned_with_tombstones_at,
        };
    }
    cleaned_ranges_with_tombstones.erase(
      tombstone_ranges->begin, std::next(tombstone_ranges->last));
    if (replacement_begin.has_value()) {
        cleaned_ranges_with_tombstones.insert(*replacement_begin);
    }
    if (replacement_last.has_value()) {
        cleaned_ranges_with_tombstones.insert(*replacement_last);
    }
    return true;
}

template<typename OffsetT>
void compaction_state<OffsetT>::truncate_with_new_start_offset(
  OffsetT new_start_offset) {
    cleaned_ranges.truncate_with_new_start_offset(new_start_offset);

    // First, remove all intervals that are fully below the new start.
    while (!cleaned_ranges_with_tombstones.empty()) {
        auto begin_it = cleaned_ranges_with_tombstones.begin();
        if (begin_it->last_offset >= new_start_offset) {
            // This interval is partially or entirely above the new start.
            // Handle below.
            break;
        }
        // This interval is entirely below the new start.
        cleaned_ranges_with_tombstones.erase(begin_it);
    }
    if (cleaned_ranges_with_tombstones.empty()) {
        return;
    }
    auto begin_it = cleaned_ranges_with_tombstones.begin();
    if (begin_it->base_offset >= new_start_offset) {
        // This interval starts above or is aligned exactly with the new start.
        return;
    }
    // This interval is partially below the new start. Replace it with an
    // interval that is aligned with the new start.
    auto truncated_begin = *begin_it;
    truncated_begin.base_offset = new_start_offset;
    cleaned_ranges_with_tombstones.erase(begin_it);
    cleaned_ranges_with_tombstones.insert(truncated_begin);
}

template<typename OffsetT>
bool compaction_state<OffsetT>::may_add(
  const cleaned_range_with_tombstones& new_range) const {
    if (cleaned_ranges_with_tombstones.empty()) {
        return true;
    }
    // Ensure that new_range doesn't overlap with an existing range.
    auto first_ge_base = cleaned_ranges_with_tombstones.lower_bound(new_range);
    if (
      first_ge_base != cleaned_ranges_with_tombstones.end()
      && first_ge_base->base_offset <= new_range.last_offset) {
        // An existing range overlaps with the new range.
        return false;
    }
    if (first_ge_base != cleaned_ranges_with_tombstones.begin()) {
        auto last_lt_base = std::prev(first_ge_base);
        if (last_lt_base->last_offset >= new_range.base_offset) {
            // An existing range overlaps with the new range.
            return false;
        }
    }
    return true;
}

template<typename OffsetT>
bool compaction_state<OffsetT>::add(
  const cleaned_range_with_tombstones& new_range) {
    if (!may_add(new_range)) {
        return false;
    }
    cleaned_ranges_with_tombstones.insert(new_range);
    return true;
}

template<typename OffsetT>
std::optional<typename compaction_state<OffsetT>::tombstone_range_iters>
compaction_state<OffsetT>::get_contiguous_range_with_tombstones(
  OffsetT base, OffsetT last) const {
    if (cleaned_ranges_with_tombstones.empty()) {
        return std::nullopt;
    }

    // Find the last interval that starts <= `base` (last_le_base). This will
    // be the interval that contains `base`, if it exists.
    auto first_gt_base = cleaned_ranges_with_tombstones.lower_bound(
      {.base_offset = next_offset(base)});
    if (first_gt_base == cleaned_ranges_with_tombstones.begin()) {
        return std::nullopt;
    }
    auto last_le_base = std::prev(first_gt_base);
    if (last_le_base->last_offset < base) {
        // The interval ends before `base`, so `base` is not covered.
        return std::nullopt;
    }
    if (last_le_base->last_offset >= last) {
        return tombstone_range_iters{
          .begin = last_le_base,
          .last = last_le_base,
        };
    }
    auto it = last_le_base;

    // Keep track of where our contiguous ranges currently end.
    auto cur_tombstone_range_last = it->last_offset;
    // Keep track of where we expect the next range to start in order to be
    // contiguous.
    auto contiguous_next_offset = next_offset(last_le_base->last_offset);

    // Iterate forward, collecting the range only if it's contiguous.
    while (++it != cleaned_ranges_with_tombstones.end()) {
        if (it->base_offset != contiguous_next_offset) {
            break;
        }
        cur_tombstone_range_last = std::max(
          it->last_offset, cur_tombstone_range_last);
        contiguous_next_offset = next_offset(it->last_offset);
        if (cur_tombstone_range_last >= last) {
            return tombstone_range_iters{
              .begin = last_le_base,
              .last = it,
            };
        }
    }
    return std::nullopt;
}

template<typename OffsetT>
compaction_offsets<OffsetT> compaction_state<OffsetT>::get_compaction_info(
  OffsetT max_removable_offset,
  OffsetT max_tombstone_remove_offset,
  std::optional<std::chrono::milliseconds> tombstone_retention_ms) const {
    compaction_offsets<OffsetT> result;

    // Compute dirty_ranges as the complement of cleaned_ranges within
    // [OffsetT{0}, max_removable_offset].
    auto offsets_stream = cleaned_ranges.make_stream();
    auto dirty_base_candidate = OffsetT{0};
    while (offsets_stream.has_next()) {
        auto cleaned_range = offsets_stream.next();
        if (cleaned_range.base_offset > max_removable_offset) {
            break;
        }
        if (cleaned_range.base_offset > dirty_base_candidate) {
            result.dirty_ranges.insert(
              dirty_base_candidate, prev_offset(cleaned_range.base_offset));
        }
        dirty_base_candidate = next_offset(cleaned_range.last_offset);
        if (dirty_base_candidate > max_removable_offset) {
            break;
        }
    }
    if (dirty_base_candidate <= max_removable_offset) {
        result.dirty_ranges.insert(dirty_base_candidate, max_removable_offset);
    }

    // Collect ranges eligible for tombstone removal.
    if (!tombstone_retention_ms.has_value()) {
        // No retention configured: tombstones are never removable.
        return result;
    }
    auto now = model::timestamp::now();
    auto retention_ms = model::timestamp(tombstone_retention_ms->count());
    // Cleaned ranges with tombstones cleaned at or before this timestamp are
    // eligible for tombstone removal.
    auto tombstone_removal_upper_bound_ts = model::timestamp(
      now.value() - retention_ms.value());
    for (const auto& r : cleaned_ranges_with_tombstones) {
        if (r.base_offset > max_tombstone_remove_offset) {
            break;
        }
        if (r.cleaned_with_tombstones_at <= tombstone_removal_upper_bound_ts) {
            auto clamped_last = std::min(
              r.last_offset, max_tombstone_remove_offset);
            result.removable_tombstone_ranges.insert(
              r.base_offset, clamped_last);
        }
    }
    return result;
}

} // namespace compaction
