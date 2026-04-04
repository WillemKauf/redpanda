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

#include "compaction/compaction_state.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "serde/rw/rw.h"

#include <gtest/gtest.h>

#include <chrono>

using namespace std::chrono_literals;

using cs = compaction::compaction_state<model::offset>;

namespace {

model::offset o(int64_t v) { return model::offset{v}; }
model::timestamp ts(int64_t v) { return model::timestamp{v}; }

cs::cleaned_range_with_tombstones
make_tombstone_range(int64_t base, int64_t last, int64_t cleaned_at) {
    return {
      .base_offset = o(base),
      .last_offset = o(last),
      .cleaned_with_tombstones_at = ts(cleaned_at),
    };
}

// Collect intervals from a compaction_offset_interval_set into pairs for
// comparison.
std::vector<std::pair<int64_t, int64_t>>
to_pairs(const compaction::compaction_offset_interval_set<model::offset>& s) {
    std::vector<std::pair<int64_t, int64_t>> result;
    auto stream = s.make_stream();
    while (stream.has_next()) {
        auto iv = stream.next();
        result.emplace_back(iv.base_offset(), iv.last_offset());
    }
    return result;
}

} // namespace

// get_compaction_info: dirty_ranges tests

TEST(CompactionState, EmptyStateHasDirtyRanges) {
    cs state;
    auto info = state.get_compaction_info(o(99), o(99), std::nullopt);
    auto dirty = to_pairs(info.dirty_ranges);
    ASSERT_EQ(dirty.size(), 1u);
    EXPECT_EQ(dirty[0].first, 0);
    EXPECT_EQ(dirty[0].second, 99);
}

TEST(CompactionState, FullyCleanedNoDirtyRanges) {
    cs state;
    state.cleaned_ranges.insert(o(0), o(99));
    auto info = state.get_compaction_info(o(99), o(99), std::nullopt);
    EXPECT_TRUE(to_pairs(info.dirty_ranges).empty());
}

TEST(CompactionState, PartiallyCleanedHasGap) {
    cs state;
    state.cleaned_ranges.insert(o(0), o(49));
    auto info = state.get_compaction_info(o(99), o(99), std::nullopt);
    auto dirty = to_pairs(info.dirty_ranges);
    ASSERT_EQ(dirty.size(), 1u);
    EXPECT_EQ(dirty[0].first, 50);
    EXPECT_EQ(dirty[0].second, 99);
}

TEST(CompactionState, CleanedMiddleHasTwoGaps) {
    cs state;
    state.cleaned_ranges.insert(o(20), o(49));
    auto info = state.get_compaction_info(o(99), o(99), std::nullopt);
    auto dirty = to_pairs(info.dirty_ranges);
    ASSERT_EQ(dirty.size(), 2u);
    EXPECT_EQ(dirty[0].first, 0);
    EXPECT_EQ(dirty[0].second, 19);
    EXPECT_EQ(dirty[1].first, 50);
    EXPECT_EQ(dirty[1].second, 99);
}

TEST(CompactionState, MaxRemovableOffsetClamping) {
    cs state;
    auto info = state.get_compaction_info(o(49), o(99), std::nullopt);
    auto dirty = to_pairs(info.dirty_ranges);
    ASSERT_EQ(dirty.size(), 1u);
    EXPECT_EQ(dirty[0].first, 0);
    EXPECT_EQ(dirty[0].second, 49);
}

TEST(CompactionState, CleanedBeyondMaxRemovableNotDirty) {
    cs state;
    state.cleaned_ranges.insert(o(0), o(99));
    auto info = state.get_compaction_info(o(49), o(99), std::nullopt);
    EXPECT_TRUE(to_pairs(info.dirty_ranges).empty());
}

// get_compaction_info: removable_tombstone_ranges tests

TEST(CompactionState, NoTombstoneRetentionNoRemovable) {
    cs state;
    state.cleaned_ranges.insert(o(0), o(99));
    ASSERT_TRUE(state.add(make_tombstone_range(0, 99, 1000)));
    auto info = state.get_compaction_info(o(99), o(99), std::nullopt);
    EXPECT_TRUE(to_pairs(info.removable_tombstone_ranges).empty());
}

TEST(CompactionState, OldTombstonesAreRemovable) {
    cs state;
    state.cleaned_ranges.insert(o(0), o(99));
    // cleaned_with_tombstones_at = 1 (epoch ms), retention = 1h.
    ASSERT_TRUE(state.add(make_tombstone_range(0, 99, 1)));
    auto info = state.get_compaction_info(o(99), o(99), 1h);
    auto removable = to_pairs(info.removable_tombstone_ranges);
    ASSERT_EQ(removable.size(), 1u);
    EXPECT_EQ(removable[0].first, 0);
    EXPECT_EQ(removable[0].second, 99);
}

TEST(CompactionState, RecentTombstonesNotRemovable) {
    cs state;
    state.cleaned_ranges.insert(o(0), o(99));
    auto now_ts = model::timestamp::now();
    ASSERT_TRUE(state.add({
      .base_offset = o(0),
      .last_offset = o(99),
      .cleaned_with_tombstones_at = now_ts,
    }));
    auto info = state.get_compaction_info(o(99), o(99), 1h);
    EXPECT_TRUE(to_pairs(info.removable_tombstone_ranges).empty());
}

TEST(CompactionState, MaxTombstoneRemoveOffsetClamping) {
    cs state;
    state.cleaned_ranges.insert(o(0), o(99));
    ASSERT_TRUE(state.add(make_tombstone_range(0, 99, 1)));
    auto info = state.get_compaction_info(o(99), o(49), 1h);
    auto removable = to_pairs(info.removable_tombstone_ranges);
    ASSERT_EQ(removable.size(), 1u);
    EXPECT_EQ(removable[0].first, 0);
    EXPECT_EQ(removable[0].second, 49);
}

// may_add / add tests

TEST(CompactionState, MayAddRejectsOverlap) {
    cs state;
    ASSERT_TRUE(state.add(make_tombstone_range(10, 50, 100)));
    EXPECT_FALSE(state.may_add(make_tombstone_range(0, 15, 100)));
    EXPECT_FALSE(state.may_add(make_tombstone_range(40, 60, 100)));
    EXPECT_FALSE(state.may_add(make_tombstone_range(20, 30, 100)));
    EXPECT_FALSE(state.may_add(make_tombstone_range(5, 55, 100)));
    // Adjacent ranges are OK.
    EXPECT_TRUE(state.may_add(make_tombstone_range(0, 9, 100)));
    EXPECT_TRUE(state.may_add(make_tombstone_range(51, 60, 100)));
}

TEST(CompactionState, AddContiguousRanges) {
    auto range = [](int base, int last) {
        return cs::cleaned_range_with_tombstones{
          .base_offset = o(base),
          .last_offset = o(last),
          .cleaned_with_tombstones_at = ts(1000),
        };
    };
    cs state;
    ASSERT_TRUE(state.may_add(range(10, 20)));
    ASSERT_TRUE(state.add(range(10, 20)));

    ASSERT_TRUE(state.may_add(range(0, 9)));
    ASSERT_TRUE(state.may_add(range(21, 30)));

    // Overlap at the edges.
    ASSERT_FALSE(state.may_add(range(0, 10)));
    ASSERT_FALSE(state.may_add(range(20, 25)));

    // Partial overlap.
    ASSERT_FALSE(state.may_add(range(5, 15)));
    ASSERT_FALSE(state.may_add(range(15, 25)));
    ASSERT_FALSE(state.may_add(range(11, 19)));

    // Full overlap.
    ASSERT_FALSE(state.may_add(range(10, 20)));
    ASSERT_FALSE(state.may_add(range(5, 25)));

    // Add another range.
    ASSERT_TRUE(state.may_add(range(30, 40)));
    ASSERT_TRUE(state.add(range(30, 40)));

    ASSERT_TRUE(state.may_add(range(21, 29)));
    ASSERT_TRUE(state.may_add(range(41, 45)));

    // Overlap at the edges.
    ASSERT_FALSE(state.may_add(range(0, 10)));
    ASSERT_FALSE(state.may_add(range(20, 30)));
    ASSERT_FALSE(state.may_add(range(40, 45)));

    // Partial overlap.
    ASSERT_FALSE(state.may_add(range(15, 25)));
    ASSERT_FALSE(state.may_add(range(25, 35)));

    // Fill the gap.
    ASSERT_TRUE(state.may_add(range(21, 29)));
    ASSERT_TRUE(state.add(range(21, 29)));

    // At this point the ranges cover [10, 40].
    for (int base = 9; base < 10; ++base) {
        for (int last = 10; last <= 40; ++last) {
            ASSERT_FALSE(state.may_add(range(base, last)));
        }
    }
    for (int base = 10; base <= 40; ++base) {
        for (int last = base; last <= 40; ++last) {
            ASSERT_FALSE(state.may_add(range(base, last)));
        }
    }
}

// has_contiguous_range_with_tombstones tests

TEST(CompactionState, HasContiguousRangeEmpty) {
    cs state;
    ASSERT_FALSE(state.has_contiguous_range_with_tombstones(o(0), o(10)));
}

TEST(CompactionState, HasContiguousRangeSingle) {
    cs state;
    ASSERT_TRUE(state.add(make_tombstone_range(0, 99, 1000)));
    EXPECT_TRUE(state.has_contiguous_range_with_tombstones(o(0), o(99)));
    EXPECT_TRUE(state.has_contiguous_range_with_tombstones(o(0), o(10)));
    EXPECT_TRUE(state.has_contiguous_range_with_tombstones(o(50), o(99)));
}

TEST(CompactionState, HasContiguousRange) {
    cs state;
    ASSERT_TRUE(state.add(make_tombstone_range(10, 49, 100)));
    ASSERT_TRUE(state.add(make_tombstone_range(50, 99, 100)));
    EXPECT_TRUE(state.has_contiguous_range_with_tombstones(o(10), o(49)));
    EXPECT_TRUE(state.has_contiguous_range_with_tombstones(o(50), o(99)));
    EXPECT_TRUE(state.has_contiguous_range_with_tombstones(o(10), o(99)));
    EXPECT_TRUE(state.has_contiguous_range_with_tombstones(o(20), o(40)));
    EXPECT_FALSE(state.has_contiguous_range_with_tombstones(o(10), o(100)));
    EXPECT_FALSE(state.has_contiguous_range_with_tombstones(o(0), o(49)));
}

TEST(CompactionState, HasContiguousRangeBaseTooLow) {
    cs state;
    ASSERT_TRUE(state.add(make_tombstone_range(5, 10, 2000)));
    for (int base = 0; base < 5; ++base) {
        for (int last = base; last < 15; ++last) {
            EXPECT_FALSE(
              state.has_contiguous_range_with_tombstones(o(base), o(last)));
        }
    }
}

TEST(CompactionState, HasContiguousRangeLastTooHigh) {
    cs state;
    ASSERT_TRUE(state.add(make_tombstone_range(5, 10, 2000)));
    for (int base = 5; base <= 10; ++base) {
        for (int last = 11; last < 15; ++last) {
            EXPECT_FALSE(
              state.has_contiguous_range_with_tombstones(o(base), o(last)));
        }
    }
}

TEST(CompactionState, HasContiguousRangeNotContiguous) {
    cs state;
    ASSERT_TRUE(state.add(make_tombstone_range(5, 10, 2000)));
    ASSERT_TRUE(state.add(make_tombstone_range(12, 20, 2000)));
    for (int base = 0; base <= 10; ++base) {
        for (int last = 12; last < 25; ++last) {
            EXPECT_FALSE(
              state.has_contiguous_range_with_tombstones(o(base), o(last)));
        }
    }
}

TEST(CompactionState, HasContiguousRangeMany) {
    cs state;
    for (int i = 0; i < 10; ++i) {
        ASSERT_TRUE(
          state.add(make_tombstone_range(i * 10, i * 10 + 9, 1000)));
    }
    ASSERT_TRUE(state.has_contiguous_range_with_tombstones(o(0), o(99)));
    ASSERT_TRUE(state.has_contiguous_range_with_tombstones(o(0), o(50)));
    ASSERT_TRUE(state.has_contiguous_range_with_tombstones(o(25), o(75)));
    ASSERT_TRUE(state.has_contiguous_range_with_tombstones(o(50), o(99)));
}

TEST(CompactionState, HasContiguousRangeManyNonUniform) {
    cs state;
    // Ranges with varying sizes, similar to real compaction output.
    for (auto [base, last] : std::initializer_list<std::pair<int, int>>{
           {120198, 120282},
           {120283, 120383},
           {120384, 120495},
           {120496, 120597},
           {120598, 120707},
           {120708, 120796},
           {120797, 120893},
           {120894, 120992},
           {120993, 121091},
           {121092, 121184},
           {121185, 121187},
           {121188, 121268},
           {121269, 121374},
           {121375, 121485},
           {121486, 121587},
           {121588, 121693},
           {121694, 121785},
           {121786, 121881},
           {121882, 121979},
         }) {
        ASSERT_TRUE(state.add(make_tombstone_range(base, last, 1000)));
    }
    ASSERT_TRUE(
      state.has_contiguous_range_with_tombstones(o(120198), o(121979)));
}

// erase_contiguous_range_with_tombstones tests

TEST(CompactionState, EraseContiguousRangeSplits) {
    cs state;
    ASSERT_TRUE(state.add(make_tombstone_range(10, 99, 100)));
    ASSERT_TRUE(state.erase_contiguous_range_with_tombstones(o(30), o(69)));
    ASSERT_EQ(state.cleaned_ranges_with_tombstones.size(), 2u);
    auto it = state.cleaned_ranges_with_tombstones.begin();
    EXPECT_EQ(it->base_offset, o(10));
    EXPECT_EQ(it->last_offset, o(29));
    ++it;
    EXPECT_EQ(it->base_offset, o(70));
    EXPECT_EQ(it->last_offset, o(99));
}

TEST(CompactionState, EraseReturnsFalseWhenNotCovered) {
    cs state;
    ASSERT_TRUE(state.add(make_tombstone_range(10, 49, 100)));
    EXPECT_FALSE(state.erase_contiguous_range_with_tombstones(o(0), o(49)));
    EXPECT_FALSE(state.erase_contiguous_range_with_tombstones(o(10), o(50)));
}

TEST(CompactionState, EraseContiguousRangeSingleStep) {
    cs state;
    ASSERT_TRUE(state.add(
      {.base_offset = o(0),
       .last_offset = o(99),
       .cleaned_with_tombstones_at = ts(1000)}));

    // Remove from the beginning.
    ASSERT_TRUE(state.erase_contiguous_range_with_tombstones(o(0), o(10)));
    {
        cs::tombstone_range_set_t expected = {
          {.base_offset = o(11),
           .last_offset = o(99),
           .cleaned_with_tombstones_at = ts(1000)},
        };
        EXPECT_EQ(state.cleaned_ranges_with_tombstones, expected);
    }

    // Remove from the end.
    ASSERT_TRUE(state.erase_contiguous_range_with_tombstones(o(89), o(99)));
    {
        cs::tombstone_range_set_t expected = {
          {.base_offset = o(11),
           .last_offset = o(88),
           .cleaned_with_tombstones_at = ts(1000)},
        };
        EXPECT_EQ(state.cleaned_ranges_with_tombstones, expected);
    }

    // Remove in the middle, creating two ranges.
    ASSERT_TRUE(state.erase_contiguous_range_with_tombstones(o(12), o(87)));
    {
        cs::tombstone_range_set_t expected = {
          {.base_offset = o(11),
           .last_offset = o(11),
           .cleaned_with_tombstones_at = ts(1000)},
          {.base_offset = o(88),
           .last_offset = o(88),
           .cleaned_with_tombstones_at = ts(1000)},
        };
        EXPECT_EQ(state.cleaned_ranges_with_tombstones, expected);
    }

    // Remove an entire range.
    ASSERT_TRUE(state.erase_contiguous_range_with_tombstones(o(88), o(88)));
    {
        cs::tombstone_range_set_t expected = {
          {.base_offset = o(11),
           .last_offset = o(11),
           .cleaned_with_tombstones_at = ts(1000)},
        };
        EXPECT_EQ(state.cleaned_ranges_with_tombstones, expected);
    }

    // Remove the last range.
    ASSERT_TRUE(state.erase_contiguous_range_with_tombstones(o(11), o(11)));
    EXPECT_TRUE(state.cleaned_ranges_with_tombstones.empty());
}

TEST(CompactionState, EraseContiguousRangeTwo) {
    cs state;
    ASSERT_TRUE(state.add(
      {.base_offset = o(0),
       .last_offset = o(49),
       .cleaned_with_tombstones_at = ts(1000)}));
    ASSERT_TRUE(state.add(
      {.base_offset = o(50),
       .last_offset = o(99),
       .cleaned_with_tombstones_at = ts(2000)}));
    ASSERT_TRUE(state.has_contiguous_range_with_tombstones(o(0), o(99)));

    // Remove a single offset.
    ASSERT_TRUE(state.erase_contiguous_range_with_tombstones(o(10), o(10)));
    {
        cs::tombstone_range_set_t expected = {
          {.base_offset = o(0),
           .last_offset = o(9),
           .cleaned_with_tombstones_at = ts(1000)},
          {.base_offset = o(11),
           .last_offset = o(49),
           .cleaned_with_tombstones_at = ts(1000)},
          {.base_offset = o(50),
           .last_offset = o(99),
           .cleaned_with_tombstones_at = ts(2000)},
        };
        EXPECT_EQ(state.cleaned_ranges_with_tombstones, expected);
    }
    ASSERT_TRUE(state.has_contiguous_range_with_tombstones(o(0), o(9)));
    ASSERT_TRUE(state.has_contiguous_range_with_tombstones(o(11), o(99)));

    // Remove a range spanning two ranges.
    ASSERT_TRUE(state.erase_contiguous_range_with_tombstones(o(11), o(89)));
    {
        cs::tombstone_range_set_t expected = {
          {.base_offset = o(0),
           .last_offset = o(9),
           .cleaned_with_tombstones_at = ts(1000)},
          {.base_offset = o(90),
           .last_offset = o(99),
           .cleaned_with_tombstones_at = ts(2000)},
        };
        EXPECT_EQ(state.cleaned_ranges_with_tombstones, expected);
    }
    ASSERT_TRUE(state.has_contiguous_range_with_tombstones(o(0), o(9)));
    ASSERT_TRUE(state.has_contiguous_range_with_tombstones(o(90), o(99)));
}

TEST(CompactionState, EraseFullManyContiguous) {
    cs state;
    for (int i = 0; i < 10; ++i) {
        ASSERT_TRUE(
          state.add(make_tombstone_range(i * 10, i * 10 + 9, 1000)));
    }
    ASSERT_TRUE(state.erase_contiguous_range_with_tombstones(o(0), o(99)));
    EXPECT_TRUE(state.cleaned_ranges_with_tombstones.empty());
}

TEST(CompactionState, EraseFullManyNonUniform) {
    cs state;
    for (auto [base, last] : std::initializer_list<std::pair<int, int>>{
           {120198, 120282},
           {120283, 120383},
           {120384, 120495},
           {120496, 120597},
           {120598, 120707},
           {120708, 120796},
           {120797, 120893},
           {120894, 120992},
           {120993, 121091},
           {121092, 121184},
           {121185, 121187},
           {121188, 121268},
           {121269, 121374},
           {121375, 121485},
           {121486, 121587},
           {121588, 121693},
           {121694, 121785},
           {121786, 121881},
           {121882, 121979},
         }) {
        ASSERT_TRUE(state.add(make_tombstone_range(base, last, 1000)));
    }
    ASSERT_TRUE(
      state.erase_contiguous_range_with_tombstones(o(120198), o(121979)));
    EXPECT_TRUE(state.cleaned_ranges_with_tombstones.empty());
}

// truncate_with_new_start_offset tests

TEST(CompactionState, TruncateEmpty) {
    cs state;
    state.truncate_with_new_start_offset(o(50));
    EXPECT_TRUE(state.cleaned_ranges_with_tombstones.empty());
}

TEST(CompactionState, TruncateRemovesAll) {
    cs state;
    ASSERT_TRUE(state.add(make_tombstone_range(10, 20, 1000)));
    ASSERT_TRUE(state.add(make_tombstone_range(30, 40, 2000)));
    state.truncate_with_new_start_offset(o(50));
    EXPECT_TRUE(state.cleaned_ranges_with_tombstones.empty());
}

TEST(CompactionState, TruncateNoOp) {
    cs state;
    ASSERT_TRUE(state.add(make_tombstone_range(10, 20, 1000)));
    ASSERT_TRUE(state.add(make_tombstone_range(30, 40, 2000)));
    auto expected = state.cleaned_ranges_with_tombstones;
    state.truncate_with_new_start_offset(o(10));
    EXPECT_EQ(state.cleaned_ranges_with_tombstones, expected);
}

TEST(CompactionState, TruncateRemovesPartialRange) {
    cs state;
    ASSERT_TRUE(state.add(make_tombstone_range(10, 20, 1000)));
    ASSERT_TRUE(state.add(make_tombstone_range(25, 30, 1500)));
    ASSERT_TRUE(state.add(make_tombstone_range(35, 45, 2000)));
    state.truncate_with_new_start_offset(o(26));
    cs::tombstone_range_set_t expected = {
      make_tombstone_range(26, 30, 1500),
      make_tombstone_range(35, 45, 2000),
    };
    EXPECT_EQ(state.cleaned_ranges_with_tombstones, expected);
}

TEST(CompactionState, TruncateExactBoundary) {
    cs state;
    ASSERT_TRUE(state.add(make_tombstone_range(10, 20, 1000)));
    ASSERT_TRUE(state.add(make_tombstone_range(30, 40, 2000)));
    state.truncate_with_new_start_offset(o(30));
    cs::tombstone_range_set_t expected = {
      make_tombstone_range(30, 40, 2000),
    };
    EXPECT_EQ(state.cleaned_ranges_with_tombstones, expected);
}

TEST(CompactionState, TruncateCleanedRanges) {
    cs state;
    state.cleaned_ranges.insert(o(10), o(20));
    state.cleaned_ranges.insert(o(25), o(35));
    state.cleaned_ranges.insert(o(40), o(50));
    ASSERT_TRUE(state.add(make_tombstone_range(10, 20, 1000)));
    ASSERT_TRUE(state.add(make_tombstone_range(25, 35, 1500)));
    ASSERT_TRUE(state.add(make_tombstone_range(40, 50, 2000)));

    state.truncate_with_new_start_offset(o(27));

    auto vec = state.cleaned_ranges.to_vec();
    ASSERT_EQ(vec.size(), 2u);
    EXPECT_EQ(vec[0].base_offset, o(27));
    EXPECT_EQ(vec[0].last_offset, o(35));
    EXPECT_EQ(vec[1].base_offset, o(40));
    EXPECT_EQ(vec[1].last_offset, o(50));

    cs::tombstone_range_set_t expected = {
      make_tombstone_range(27, 35, 1500),
      make_tombstone_range(40, 50, 2000),
    };
    EXPECT_EQ(state.cleaned_ranges_with_tombstones, expected);
}

TEST(CompactionState, TruncateWithNewStartOffset) {
    cs state;
    state.cleaned_ranges.insert(o(0), o(99));
    ASSERT_TRUE(state.add(make_tombstone_range(0, 49, 100)));
    ASSERT_TRUE(state.add(make_tombstone_range(50, 99, 200)));
    state.truncate_with_new_start_offset(o(30));

    auto cleaned = to_pairs(state.cleaned_ranges);
    ASSERT_EQ(cleaned.size(), 1u);
    EXPECT_EQ(cleaned[0].first, 30);
    EXPECT_EQ(cleaned[0].second, 99);

    ASSERT_EQ(state.cleaned_ranges_with_tombstones.size(), 2u);
    auto it = state.cleaned_ranges_with_tombstones.begin();
    EXPECT_EQ(it->base_offset, o(30));
    EXPECT_EQ(it->last_offset, o(49));
    ++it;
    EXPECT_EQ(it->base_offset, o(50));
    EXPECT_EQ(it->last_offset, o(99));
}

TEST(CompactionState, TruncateRemovesFullyBelowRanges) {
    cs state;
    state.cleaned_ranges.insert(o(0), o(99));
    ASSERT_TRUE(state.add(make_tombstone_range(0, 19, 100)));
    ASSERT_TRUE(state.add(make_tombstone_range(20, 99, 200)));
    state.truncate_with_new_start_offset(o(50));
    ASSERT_EQ(state.cleaned_ranges_with_tombstones.size(), 1u);
    auto it = state.cleaned_ranges_with_tombstones.begin();
    EXPECT_EQ(it->base_offset, o(50));
    EXPECT_EQ(it->last_offset, o(99));
}

// Serde roundtrip

TEST(CompactionState, SerdeRoundtrip) {
    cs state;
    state.cleaned_ranges.insert(o(0), o(49));
    state.cleaned_ranges.insert(o(60), o(99));
    ASSERT_TRUE(state.add(make_tombstone_range(0, 49, 100)));
    ASSERT_TRUE(state.add(make_tombstone_range(60, 99, 200)));

    auto buf = serde::to_iobuf(state);
    auto restored = serde::from_iobuf<cs>(std::move(buf));
    EXPECT_EQ(state, restored);
}
