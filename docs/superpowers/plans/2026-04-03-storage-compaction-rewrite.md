# Storage Compaction Rewrite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rewrite storage compaction to use the shared `compaction::sliding_window_reducer` two-pass architecture (source/sink/filter), matching the cloud_topics L1 compaction pattern, with persistent compaction state and a new crash-safe segment replacement API.

**Architecture:** A `compaction_worker` orchestrates the `sliding_window_reducer` with a local-storage `compaction_source` (reads segments via log reader), `compaction_sink` (writes to `segment_appender`, calls `replace_offset_range()`), and `compaction_filter` (dedup + tombstone removal). Compaction state (cleaned ranges, tombstone tracking) is persisted as a serde file in the partition directory.

**Tech Stack:** C++23, Seastar, serde serialization, Bazel build, Google Test

**Spec:** `docs/superpowers/specs/2026-04-03-storage-compaction-rewrite-design.md`

---

## File Structure

### New files

| File | Responsibility |
|------|---------------|
| `src/v/container/offset_interval_set.h` | Templated offset interval set (moved from L1, generalized) |
| `src/v/container/offset_interval_set.cc` | Implementation |
| `src/v/storage/compaction/compaction_state.h` | Compaction state struct + serde + helpers |
| `src/v/storage/compaction/compaction_state.cc` | Helper method implementations + persistence |
| `src/v/storage/compaction/compaction_filter.h` | `compaction::filter` subclass for local storage |
| `src/v/storage/compaction/compaction_filter.cc` | Filter implementation |
| `src/v/storage/compaction/compaction_sink.h` | `sliding_window_reducer::sink` for local segments |
| `src/v/storage/compaction/compaction_sink.cc` | Sink implementation |
| `src/v/storage/compaction/compaction_source.h` | `sliding_window_reducer::source` for local segments |
| `src/v/storage/compaction/compaction_source.cc` | Source implementation |
| `src/v/storage/compaction/compaction_worker.h` | Orchestrator, owns key_offset_map |
| `src/v/storage/compaction/compaction_worker.cc` | Worker implementation |
| `src/v/storage/compaction/BUILD` | Bazel build definitions |
| `src/v/storage/compaction/tests/BUILD` | Test build definitions |
| `src/v/storage/compaction/tests/compaction_state_test.cc` | State unit tests |
| `src/v/storage/compaction/tests/compaction_filter_test.cc` | Filter unit tests |

### Modified files

| File | Change |
|------|--------|
| `src/v/cloud_topics/level_one/metastore/BUILD` | Remove `offset_interval_set` target |
| `src/v/cloud_topics/level_one/metastore/offset_interval_set.h` | Replace with include redirect to new location |
| `src/v/container/BUILD` | Add `offset_interval_set` target |
| `src/v/storage/disk_log_impl.h` | Add `replace_offset_range()`, `compaction_state` member |
| `src/v/storage/disk_log_impl.cc` | Implement `replace_offset_range()`, startup overlap recovery |

---

## Implementation Notes

**Shared interface limitation:** The `sliding_window_reducer::sink::prepare_iteration(kafka::offset)`
takes only an offset. Our sink needs `model::term_id` too for rolling. The
recommended approach: add a `set_current_term(model::term_id)` method on the
sink that the source calls before `prepare_iteration()`. This avoids modifying
the shared interface. Alternatively, extend the shared interface to take an
optional term parameter — coordinate with the L1 team if taking this approach.

**Skeleton tests:** Tasks 3 and 9 contain test skeletons with TODO bodies.
The implementor should fill these in using the batch generation utilities
(`tests::kv_t::sequence`, `model::test::make_random_batch`) following the
patterns in `src/v/cloud_topics/level_one/compaction/tests/reducer_test.cc`.

**Sink staging segments:** The sink's `initialize_appender()` and
`flush_and_replace()` have TODO comments. The implementor should follow
the pattern in `segment_utils.cc:make_segment_appender()` for creating
staging files and `segment_utils.cc:concatenate_and_rebuild_target_segment()`
for the segment finalization pattern.

---

## Task 1: Move `offset_interval_set` to `src/v/container/`

The current `offset_interval_set` is hardcoded to `kafka::offset`. We need to
templatize it so storage compaction can use it with `model::offset`.

**Files:**
- Create: `src/v/container/offset_interval_set.h`
- Create: `src/v/container/offset_interval_set.cc`
- Modify: `src/v/container/BUILD`
- Modify: `src/v/cloud_topics/level_one/metastore/offset_interval_set.h`
- Modify: `src/v/cloud_topics/level_one/metastore/offset_interval_set.cc`
- Modify: `src/v/cloud_topics/level_one/metastore/BUILD`
- Modify: All files that include the old path (20+ files)

- [ ] **Step 1: Create templated `offset_interval_set` in `src/v/container/`**

Create `src/v/container/offset_interval_set.h`:

```cpp
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

#include "container/chunked_vector.h"
#include "container/interval_set.h"
#include "model/fundamental.h"
#include "serde/envelope.h"
#include "serde/rw/envelope.h"
#include "serde/rw/map.h"

#include <ostream>

// Wrapper around an interval_set, but with an interface that makes it
// conducive to using inclusive offset ranges.
//
// OffsetT must be a named_type wrapping an integral (e.g. kafka::offset,
// model::offset).
template<typename OffsetT>
class offset_interval_set
  : public serde::envelope<
      offset_interval_set<OffsetT>,
      serde::version<0>,
      serde::compat_version<0>> {
public:
    using offset_t = OffsetT;
    using iset_t = interval_set<typename OffsetT::type>;
    bool operator==(const offset_interval_set&) const = default;
    auto serde_fields() { return std::tie(iset_); }

    struct interval {
        OffsetT base_offset;
        OffsetT last_offset;

        friend std::ostream& operator<<(std::ostream& o, const interval& iv) {
            fmt::print(o, "{}", iv);
            return o;
        }
    };

    template<bool reverse = false>
    class stream {
    public:
        using iterator_t = std::conditional_t<
          reverse,
          typename iset_t::const_reverse_iterator,
          typename iset_t::const_iterator>;

        explicit stream(const iset_t& underlying)
          : set_(underlying) {
            if constexpr (reverse) {
                iter_ = set_.rbegin();
                end_ = set_.rend();
            } else {
                iter_ = set_.begin();
                end_ = set_.end();
            }
        }
        bool has_next() const noexcept { return iter_ != end_; }
        interval next() {
            vassert(has_next(), "next() called while has_next() is false");
            interval ret{
              .base_offset = OffsetT(iter_->first),
              .last_offset = OffsetT(iter_->second - 1),
            };
            ++iter_;
            return ret;
        }

    private:
        const iset_t& set_;
        iterator_t iter_;
        iterator_t end_;
    };

    bool empty() const { return iset_.empty(); }

    bool insert(OffsetT base, OffsetT last) {
        auto len = last() - base() + 1;
        return iset_.insert(typename iset_t::interval{base(), len}).second;
    }

    bool contains(OffsetT o) const {
        return iset_.find(o()) != iset_.end();
    }

    bool covers(OffsetT start, OffsetT end) const {
        auto it = iset_.find(start());
        if (it == iset_.end()) {
            return false;
        }
        return (it->first <= start() && it->second > end());
    }

    stream<false> make_stream() const { return stream(iset_); }
    stream<true> make_reverse_stream() const { return stream<true>(iset_); }

    chunked_vector<interval> to_vec() const {
        chunked_vector<interval> ret;
        ret.reserve(iset_.size());
        auto s = make_stream();
        while (s.has_next()) {
            ret.emplace_back(s.next());
        }
        return ret;
    }

    void truncate_with_new_start_offset(OffsetT new_start_offset) {
        while (!iset_.empty()) {
            auto begin_it = iset_.begin();
            auto begin_last_offset = OffsetT{iset_.to_end(begin_it) - 1};
            if (begin_last_offset >= new_start_offset) {
                break;
            }
            iset_.erase(begin_it);
        }
        if (iset_.empty()) {
            return;
        }
        auto begin_it = iset_.begin();
        auto begin_base_offset = OffsetT{iset_.to_start(begin_it)};
        if (begin_base_offset >= new_start_offset) {
            return;
        }
        auto begin_last_offset = OffsetT{iset_.to_end(begin_it) - 1};
        iset_.erase(begin_it);
        insert(new_start_offset, begin_last_offset);
    }

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{}", iset_);
    }

private:
    interval_set<typename OffsetT::type> iset_;
};

template<typename OffsetT>
struct fmt::formatter<typename offset_interval_set<OffsetT>::interval> final
  : fmt::formatter<std::string_view> {
    template<typename FormatContext>
    auto format(
      const typename offset_interval_set<OffsetT>::interval& iv,
      FormatContext& ctx) const {
        return fmt::format_to(
          ctx.out(), "[{}, {}]", iv.base_offset, iv.last_offset);
    }
};
```

Since this is now a template, the implementation is header-only. Create an
empty `src/v/container/offset_interval_set.cc` with just the copyright header
(needed if we want a cc_library target, or we can make it header-only).

Actually, since the class is now fully templated and header-only, we don't need
a `.cc` file. The Bazel target can be header-only.

- [ ] **Step 2: Add Bazel target in `src/v/container/BUILD`**

Add to `src/v/container/BUILD`:

```python
redpanda_cc_library(
    name = "offset_interval_set",
    hdrs = [
        "offset_interval_set.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":chunked_vector",
        ":interval_set",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/serde:map",
    ],
)
```

- [ ] **Step 3: Replace L1's `offset_interval_set.h` with a redirect**

Replace the contents of
`src/v/cloud_topics/level_one/metastore/offset_interval_set.h` with:

```cpp
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

// This header has moved to container/offset_interval_set.h.
// This redirect exists to avoid updating all includes at once.
#include "container/offset_interval_set.h"

namespace cloud_topics::l1 {

// Type alias preserving the original L1 type name.
using offset_interval_set = ::offset_interval_set<kafka::offset>;

} // namespace cloud_topics::l1
```

Delete `src/v/cloud_topics/level_one/metastore/offset_interval_set.cc` (no
longer needed since the template is header-only).

- [ ] **Step 4: Update L1 metastore BUILD**

In `src/v/cloud_topics/level_one/metastore/BUILD`, update the
`offset_interval_set` target to be header-only with a dep on the container
target:

```python
redpanda_cc_library(
    name = "offset_interval_set",
    hdrs = [
        "offset_interval_set.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/container:offset_interval_set",
    ],
)
```

Remove `offset_interval_set.cc` from the `srcs` list and the old deps that are
now pulled transitively.

- [ ] **Step 5: Build and verify**

Run:
```bash
bazel build //src/v/container:offset_interval_set
bazel build //src/v/cloud_topics/level_one/metastore:offset_interval_set
bazel build //src/v/cloud_topics/level_one/compaction:source_and_sink
bazel build //src/v/cloud_topics/level_one/compaction:filter
```

Expected: All targets build successfully. The L1 code should work unchanged
because the type alias preserves the original `cloud_topics::l1::offset_interval_set`
name.

- [ ] **Step 6: Run existing offset_interval_set tests**

Run:
```bash
bazel test //src/v/cloud_topics/level_one/metastore/tests:offset_interval_set_test
```

Expected: All tests pass. The behavior is identical — only the location changed.

- [ ] **Step 7: Commit**

```bash
git add src/v/container/offset_interval_set.h src/v/container/BUILD \
  src/v/cloud_topics/level_one/metastore/offset_interval_set.h \
  src/v/cloud_topics/level_one/metastore/BUILD
git rm src/v/cloud_topics/level_one/metastore/offset_interval_set.cc
git commit -m "container: move offset_interval_set to shared location

Templatize offset_interval_set on offset type and move to
src/v/container/ so it can be reused by storage compaction with
model::offset. L1 code uses a type alias redirect so no include
changes are needed in the L1 codebase."
```

---

## Task 2: Compaction State

Persistent compaction state tracking cleaned ranges and tombstone information,
modeled after L1's `compaction_state` in `state.h` / `state.cc`.

**Files:**
- Create: `src/v/storage/compaction/compaction_state.h`
- Create: `src/v/storage/compaction/compaction_state.cc`
- Create: `src/v/storage/compaction/BUILD`
- Test: `src/v/storage/compaction/tests/compaction_state_test.cc`
- Create: `src/v/storage/compaction/tests/BUILD`

- [ ] **Step 1: Create the BUILD file for `src/v/storage/compaction/`**

Create `src/v/storage/compaction/BUILD`:

```python
load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/storage:__subpackages__"])

redpanda_cc_library(
    name = "compaction_state",
    srcs = [
        "compaction_state.cc",
    ],
    hdrs = [
        "compaction_state.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/container:offset_interval_set",
        "//src/v/model",
        "//src/v/serde",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)
```

- [ ] **Step 2: Create the test BUILD file**

Create `src/v/storage/compaction/tests/BUILD`:

```python
load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "compaction_state_test",
    timeout = "short",
    srcs = [
        "compaction_state_test.cc",
    ],
    deps = [
        "//src/v/storage/compaction:compaction_state",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
```

- [ ] **Step 3: Write the compaction_state tests**

Create `src/v/storage/compaction/tests/compaction_state_test.cc`:

```cpp
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/compaction/compaction_state.h"

#include "model/fundamental.h"
#include "model/timestamp.h"

#include <gtest/gtest.h>

using namespace storage::compaction;

TEST(CompactionStateTest, EmptyStateHasNoDirtyRanges) {
    compaction_state state;
    auto info = state.get_compaction_info(
      model::offset{100},
      model::offset{100},
      std::nullopt);
    // With no cleaned ranges, the full log range is dirty.
    EXPECT_FALSE(info.dirty_ranges.empty());
}

TEST(CompactionStateTest, FullyCleanedHasNoDirtyRanges) {
    compaction_state state;
    state.cleaned_ranges.insert(model::offset{0}, model::offset{100});
    auto info = state.get_compaction_info(
      model::offset{100},
      model::offset{100},
      std::nullopt);
    EXPECT_TRUE(info.dirty_ranges.empty());
}

TEST(CompactionStateTest, PartiallyCleanedHasDirtyGap) {
    compaction_state state;
    state.cleaned_ranges.insert(model::offset{0}, model::offset{49});
    auto info = state.get_compaction_info(
      model::offset{100},
      model::offset{100},
      std::nullopt);
    EXPECT_FALSE(info.dirty_ranges.empty());
    auto vec = info.dirty_ranges.to_vec();
    ASSERT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0].base_offset, model::offset{50});
    EXPECT_EQ(vec[0].last_offset, model::offset{100});
}

TEST(CompactionStateTest, MaxRemovableClampsDirtyRanges) {
    compaction_state state;
    auto info = state.get_compaction_info(
      model::offset{50},  // max_removable_offset
      model::offset{100}, // max_tombstone_remove_offset
      std::nullopt);
    auto vec = info.dirty_ranges.to_vec();
    ASSERT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0].last_offset, model::offset{50});
}

TEST(CompactionStateTest, RemovableTombstoneRangesRespectRetention) {
    compaction_state state;
    state.cleaned_ranges.insert(model::offset{0}, model::offset{100});

    // Cleaned 10 seconds ago
    auto ts = model::timestamp(model::timestamp::now()() - 10000);
    compaction_state::cleaned_range_with_tombstones range{
      .base_offset = model::offset{0},
      .last_offset = model::offset{100},
      .cleaned_with_tombstones_at = ts,
    };
    EXPECT_TRUE(state.add(range));

    // With 5s retention, tombstones are removable
    auto info = state.get_compaction_info(
      model::offset{100},
      model::offset{100},
      std::chrono::milliseconds{5000});
    EXPECT_FALSE(info.removable_tombstone_ranges.empty());

    // With 60s retention, tombstones are NOT yet removable
    auto info2 = state.get_compaction_info(
      model::offset{100},
      model::offset{100},
      std::chrono::milliseconds{60000});
    EXPECT_TRUE(info2.removable_tombstone_ranges.empty());
}

TEST(CompactionStateTest, MayAddRejectsOverlapping) {
    compaction_state state;
    compaction_state::cleaned_range_with_tombstones r1{
      .base_offset = model::offset{10},
      .last_offset = model::offset{50},
      .cleaned_with_tombstones_at = model::timestamp::now(),
    };
    EXPECT_TRUE(state.add(r1));
    compaction_state::cleaned_range_with_tombstones r2{
      .base_offset = model::offset{40},
      .last_offset = model::offset{60},
      .cleaned_with_tombstones_at = model::timestamp::now(),
    };
    EXPECT_FALSE(state.may_add(r2));
    EXPECT_FALSE(state.add(r2));
}

TEST(CompactionStateTest, EraseContiguousRangeWithTombstones) {
    compaction_state state;
    state.add({
      .base_offset = model::offset{10},
      .last_offset = model::offset{99},
      .cleaned_with_tombstones_at = model::timestamp(900),
    });
    state.add({
      .base_offset = model::offset{100},
      .last_offset = model::offset{199},
      .cleaned_with_tombstones_at = model::timestamp(1000),
    });
    // Erase middle range [80, 129] which spans two entries
    EXPECT_TRUE(state.erase_contiguous_range_with_tombstones(
      model::offset{80}, model::offset{129}));
    // Should leave [10,79] and [130,199]
    EXPECT_TRUE(state.has_contiguous_range_with_tombstones(
      model::offset{10}, model::offset{79}));
    EXPECT_TRUE(state.has_contiguous_range_with_tombstones(
      model::offset{130}, model::offset{199}));
    EXPECT_FALSE(state.has_contiguous_range_with_tombstones(
      model::offset{80}, model::offset{100}));
}

TEST(CompactionStateTest, TruncateWithNewStartOffset) {
    compaction_state state;
    state.cleaned_ranges.insert(model::offset{0}, model::offset{100});
    state.add({
      .base_offset = model::offset{0},
      .last_offset = model::offset{100},
      .cleaned_with_tombstones_at = model::timestamp::now(),
    });
    state.truncate_with_new_start_offset(model::offset{50});
    auto vec = state.cleaned_ranges.to_vec();
    ASSERT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0].base_offset, model::offset{50});
    EXPECT_EQ(vec[0].last_offset, model::offset{100});
}

TEST(CompactionStateTest, SerdeRoundTrip) {
    compaction_state state;
    state.cleaned_ranges.insert(model::offset{0}, model::offset{50});
    state.cleaned_ranges.insert(model::offset{100}, model::offset{200});
    state.add({
      .base_offset = model::offset{0},
      .last_offset = model::offset{50},
      .cleaned_with_tombstones_at = model::timestamp(12345),
    });

    auto buf = serde::to_iobuf(state);
    auto deserialized = serde::from_iobuf<compaction_state>(std::move(buf));

    EXPECT_EQ(state, deserialized);
}
```

- [ ] **Step 4: Run tests to verify they fail**

Run:
```bash
bazel test //src/v/storage/compaction/tests:compaction_state_test
```

Expected: BUILD ERROR — `compaction_state.h` doesn't exist yet.

- [ ] **Step 5: Create `compaction_state.h`**

Create `src/v/storage/compaction/compaction_state.h`:

```cpp
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "container/offset_interval_set.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "serde/envelope.h"
#include "serde/rw/envelope.h"

#include <absl/container/btree_set.h>

#include <chrono>
#include <optional>

namespace storage::compaction {

using model_offset_interval_set = offset_interval_set<model::offset>;

struct compaction_offsets {
    model_offset_interval_set dirty_ranges;
    model_offset_interval_set removable_tombstone_ranges;
};

struct compaction_state
  : serde::envelope<
      compaction_state,
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

        model::offset base_offset;
        model::offset last_offset;
        model::timestamp cleaned_with_tombstones_at;
    };
    using tombstone_range_set_t
      = absl::btree_set<cleaned_range_with_tombstones>;

    friend bool operator==(const compaction_state&, const compaction_state&)
      = default;
    auto serde_fields() {
        return std::tie(cleaned_ranges, cleaned_ranges_with_tombstones);
    }

    /// Returns false if the input range overlaps with an existing range.
    bool may_add(const cleaned_range_with_tombstones&) const;

    /// Adds the input range. Returns false if it overlaps.
    bool add(const cleaned_range_with_tombstones&);

    /// Returns true if the inclusive range is fully covered by contiguous
    /// cleaned ranges with tombstones.
    bool has_contiguous_range_with_tombstones(
      model::offset, model::offset) const;

    /// Removes the inclusive range from cleaned_ranges_with_tombstones.
    bool erase_contiguous_range_with_tombstones(
      model::offset, model::offset);

    /// Prefix-truncates both cleaned_ranges and cleaned_ranges_with_tombstones.
    void truncate_with_new_start_offset(model::offset);

    /// Computes dirty ranges and removable tombstone ranges.
    /// max_removable_offset: upper bound for compactable offsets
    /// max_tombstone_remove_offset: upper bound for tombstone removal
    /// tombstone_retention_ms: delete.retention.ms (nullopt = don't remove)
    compaction_offsets get_compaction_info(
      model::offset max_removable_offset,
      model::offset max_tombstone_remove_offset,
      std::optional<std::chrono::milliseconds> tombstone_retention_ms) const;

    model_offset_interval_set cleaned_ranges;
    tombstone_range_set_t cleaned_ranges_with_tombstones;

private:
    struct tombstone_range_iters {
        tombstone_range_set_t::const_iterator begin;
        tombstone_range_set_t::const_iterator last;
    };
    std::optional<tombstone_range_iters>
      get_contiguous_range_with_tombstones(
        model::offset, model::offset) const;
};

} // namespace storage::compaction
```

- [ ] **Step 6: Create `compaction_state.cc`**

Create `src/v/storage/compaction/compaction_state.cc`:

```cpp
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/compaction/compaction_state.h"

namespace storage::compaction {

bool compaction_state::may_add(
  const cleaned_range_with_tombstones& new_range) const {
    if (cleaned_ranges_with_tombstones.empty()) {
        return true;
    }
    auto first_ge_base = cleaned_ranges_with_tombstones.lower_bound(new_range);
    if (
      first_ge_base != cleaned_ranges_with_tombstones.end()
      && first_ge_base->base_offset <= new_range.last_offset) {
        return false;
    }
    if (first_ge_base != cleaned_ranges_with_tombstones.begin()) {
        auto last_lt_base = std::prev(first_ge_base);
        if (last_lt_base->last_offset >= new_range.base_offset) {
            return false;
        }
    }
    return true;
}

bool compaction_state::add(const cleaned_range_with_tombstones& new_range) {
    if (!may_add(new_range)) {
        return false;
    }
    cleaned_ranges_with_tombstones.insert(new_range);
    return true;
}

bool compaction_state::has_contiguous_range_with_tombstones(
  model::offset base_offset, model::offset last_offset) const {
    return get_contiguous_range_with_tombstones(base_offset, last_offset)
      .has_value();
}

bool compaction_state::erase_contiguous_range_with_tombstones(
  model::offset base_offset, model::offset last_offset) {
    auto tombstone_ranges = get_contiguous_range_with_tombstones(
      base_offset, last_offset);
    if (!tombstone_ranges.has_value()) {
        return false;
    }
    std::optional<cleaned_range_with_tombstones> replacement_begin;
    if (tombstone_ranges->begin->base_offset != base_offset) {
        replacement_begin = cleaned_range_with_tombstones{
          .base_offset = tombstone_ranges->begin->base_offset,
          .last_offset = model::prev_offset(base_offset),
          .cleaned_with_tombstones_at
          = tombstone_ranges->begin->cleaned_with_tombstones_at,
        };
    }
    std::optional<cleaned_range_with_tombstones> replacement_last;
    if (tombstone_ranges->last->last_offset != last_offset) {
        replacement_last = cleaned_range_with_tombstones{
          .base_offset = model::next_offset(last_offset),
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

void compaction_state::truncate_with_new_start_offset(
  model::offset new_start_offset) {
    cleaned_ranges.truncate_with_new_start_offset(new_start_offset);

    while (!cleaned_ranges_with_tombstones.empty()) {
        auto begin_it = cleaned_ranges_with_tombstones.begin();
        if (begin_it->last_offset >= new_start_offset) {
            break;
        }
        cleaned_ranges_with_tombstones.erase(begin_it);
    }
    if (cleaned_ranges_with_tombstones.empty()) {
        return;
    }
    auto begin_it = cleaned_ranges_with_tombstones.begin();
    if (begin_it->base_offset >= new_start_offset) {
        return;
    }
    auto truncated_begin = *begin_it;
    truncated_begin.base_offset = new_start_offset;
    cleaned_ranges_with_tombstones.erase(begin_it);
    cleaned_ranges_with_tombstones.insert(truncated_begin);
}

std::optional<compaction_state::tombstone_range_iters>
compaction_state::get_contiguous_range_with_tombstones(
  model::offset base, model::offset last) const {
    if (cleaned_ranges_with_tombstones.empty()) {
        return std::nullopt;
    }
    auto first_gt_base = cleaned_ranges_with_tombstones.lower_bound(
      {.base_offset = model::next_offset(base)});
    if (first_gt_base == cleaned_ranges_with_tombstones.begin()) {
        return std::nullopt;
    }
    auto last_le_base = std::prev(first_gt_base);
    if (last_le_base->last_offset >= last) {
        return tombstone_range_iters{
          .begin = last_le_base,
          .last = last_le_base,
        };
    }
    auto it = last_le_base;
    auto cur_tombstone_range_last = it->last_offset;
    auto contiguous_next_offset = model::next_offset(last_le_base->last_offset);
    while (++it != cleaned_ranges_with_tombstones.end()) {
        if (it->base_offset != contiguous_next_offset) {
            break;
        }
        cur_tombstone_range_last = std::max(
          it->last_offset, cur_tombstone_range_last);
        contiguous_next_offset = model::next_offset(it->last_offset);
        if (cur_tombstone_range_last >= last) {
            return tombstone_range_iters{
              .begin = last_le_base,
              .last = it,
            };
        }
    }
    return std::nullopt;
}

compaction_offsets compaction_state::get_compaction_info(
  model::offset max_removable_offset,
  model::offset max_tombstone_remove_offset,
  std::optional<std::chrono::milliseconds> tombstone_retention_ms) const {
    compaction_offsets result;

    // Dirty ranges = [0, max_removable_offset] minus cleaned_ranges.
    model_offset_interval_set full_range;
    full_range.insert(model::offset{0}, max_removable_offset);
    // Subtract cleaned ranges by iterating and inserting only uncovered parts.
    auto stream = cleaned_ranges.make_stream();
    model_offset_interval_set uncleaned;
    model::offset cursor{0};
    while (stream.has_next()) {
        auto iv = stream.next();
        if (iv.base_offset > max_removable_offset) {
            break;
        }
        if (iv.base_offset > cursor) {
            uncleaned.insert(
              cursor,
              model::prev_offset(
                std::min(iv.base_offset, model::next_offset(max_removable_offset))));
        }
        cursor = model::next_offset(
          std::min(iv.last_offset, max_removable_offset));
    }
    if (cursor <= max_removable_offset) {
        uncleaned.insert(cursor, max_removable_offset);
    }
    result.dirty_ranges = std::move(uncleaned);

    // Removable tombstone ranges: cleaned ranges with tombstones where
    // now - cleaned_at >= tombstone_retention_ms, clamped to
    // max_tombstone_remove_offset.
    if (tombstone_retention_ms.has_value()) {
        auto now = model::timestamp::now();
        for (const auto& r : cleaned_ranges_with_tombstones) {
            if (r.base_offset > max_tombstone_remove_offset) {
                break;
            }
            auto age_ms = now() - r.cleaned_with_tombstones_at();
            if (age_ms >= tombstone_retention_ms->count()) {
                result.removable_tombstone_ranges.insert(
                  r.base_offset,
                  std::min(r.last_offset, max_tombstone_remove_offset));
            }
        }
    }

    return result;
}

} // namespace storage::compaction
```

- [ ] **Step 7: Run tests to verify they pass**

Run:
```bash
bazel test //src/v/storage/compaction/tests:compaction_state_test
```

Expected: All tests pass.

- [ ] **Step 8: Commit**

```bash
git add src/v/storage/compaction/
git commit -m "storage/compaction: add compaction_state with persistence support

Persistent state tracking cleaned offset ranges and tombstone
information for the new storage compaction pipeline. Modeled after
the L1 metastore compaction_state, adapted for model::offset."
```

---

## Task 3: Compaction Filter

Extends `compaction::filter` for local storage dedup + tombstone removal.
Creates placeholder batches when all records in a batch are filtered out
(local storage needs contiguous offset space).

**Files:**
- Create: `src/v/storage/compaction/compaction_filter.h`
- Create: `src/v/storage/compaction/compaction_filter.cc`
- Modify: `src/v/storage/compaction/BUILD`
- Test: `src/v/storage/compaction/tests/compaction_filter_test.cc`
- Modify: `src/v/storage/compaction/tests/BUILD`

- [ ] **Step 1: Write filter tests**

Create `src/v/storage/compaction/tests/compaction_filter_test.cc`:

```cpp
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/compaction/compaction_filter.h"

#include "compaction/key_offset_map.h"
#include "container/offset_interval_set.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/tests/random_batch.h"

#include <gtest/gtest.h>

using namespace storage::compaction;

namespace {

// Minimal sink that just collects batches.
class collecting_sink final
  : public ::compaction::sliding_window_reducer::sink {
public:
    ss::future<bool> initialize(
      ::compaction::sliding_window_reducer::source&) final {
        co_return true;
    }
    ss::future<ss::stop_iteration>
    operator()(model::record_batch b, model::compression) final {
        batches.push_back(std::move(b));
        co_return ss::stop_iteration::no;
    }
    ss::future<> finalize(bool) final { co_return; }
    ss::future<> prepare_iteration(kafka::offset) final { co_return; }
    ss::future<>
    finish_iteration(kafka::offset, kafka::offset) final { co_return; }

    std::vector<model::record_batch> batches;
};

} // anonymous namespace

TEST(CompactionFilterTest, KeepsLatestRecordForKey) {
    // This test verifies that the filter keeps only the latest version of
    // each key according to the key_offset_map.
    // Detailed test implementation will depend on the batch generation
    // utilities available. The key behavior to verify:
    // 1. Records whose offset matches the map entry are kept
    // 2. Records with older offsets for the same key are discarded
    // 3. Records not in the map at all are kept (they are the latest)
}

TEST(CompactionFilterTest, RemovesTombstonesInRemovableRange) {
    // Verifies that tombstone records within removable_tombstone_ranges
    // are discarded, while tombstones outside those ranges are kept.
}

TEST(CompactionFilterTest, EmptyBatchBecomesPlaceholder) {
    // Verifies that when all records in a batch are filtered out,
    // a compaction placeholder batch is created instead of dropping
    // the batch entirely (local storage needs contiguous offsets).
}
```

Note: The test bodies are skeleton-only because they depend on batch generation
utilities that vary. The implementor should fill these in using
`model::test::make_random_batch()` and `tests::kv_t::sequence()` patterns from
`src/v/model/tests/random_batch.h` and
`src/v/kafka/server/tests/produce_consume_utils.h`.

- [ ] **Step 2: Add filter target to BUILD**

Add to `src/v/storage/compaction/BUILD`:

```python
redpanda_cc_library(
    name = "compaction_filter",
    srcs = [
        "compaction_filter.cc",
    ],
    hdrs = [
        "compaction_filter.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":compaction_state",
        "//src/v/compaction:filter",
        "//src/v/compaction:key_offset_map",
        "//src/v/compaction:utils",
        "//src/v/container:offset_interval_set",
        "//src/v/model",
        "//src/v/storage:storage",
        "@seastar",
    ],
)
```

Add to `src/v/storage/compaction/tests/BUILD`:

```python
redpanda_cc_gtest(
    name = "compaction_filter_test",
    timeout = "short",
    srcs = [
        "compaction_filter_test.cc",
    ],
    deps = [
        "//src/v/storage/compaction:compaction_filter",
        "//src/v/compaction:key_offset_map",
        "//src/v/compaction:reducer",
        "//src/v/container:offset_interval_set",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
```

- [ ] **Step 3: Create `compaction_filter.h`**

Create `src/v/storage/compaction/compaction_filter.h`:

```cpp
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
#include "container/offset_interval_set.h"
#include "model/fundamental.h"
#include "model/record.h"

#include <seastar/core/future.hh>

#include <vector>

namespace storage::compaction {

using model_offset_interval_set = offset_interval_set<model::offset>;

class compaction_filter final : public ::compaction::filter {
public:
    compaction_filter(
      ::compaction::sliding_window_reducer::sink& sink,
      const ::compaction::key_offset_map& map,
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

    const ::compaction::key_offset_map& _map;
    const model_offset_interval_set& _removable_tombstone_ranges;
};

} // namespace storage::compaction
```

- [ ] **Step 4: Create `compaction_filter.cc`**

Create `src/v/storage/compaction/compaction_filter.cc`:

```cpp
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/compaction/compaction_filter.h"

#include "compaction/utils.h"
#include "model/record.h"
#include "storage/compaction_reducers.h"

namespace storage::compaction {

compaction_filter::compaction_filter(
  ::compaction::sliding_window_reducer::sink& sink,
  const ::compaction::key_offset_map& map,
  model::ntp ntp,
  const model_offset_interval_set& removable_tombstone_ranges)
  : ::compaction::filter(sink, std::move(ntp))
  , _map(map)
  , _removable_tombstone_ranges(removable_tombstone_ranges) {}

ss::future<bool> compaction_filter::should_keep(
  const model::record_batch& b, const model::record& r) const {
    if (r.is_tombstone()) {
        auto o = b.base_offset() + model::offset_delta(r.offset_delta());
        if (_removable_tombstone_ranges.contains(model::offset(o))) {
            ++_stats.expired_tombstones_discarded;
            co_return false;
        }
    }
    co_return co_await ::compaction::is_latest_record_for_key(_map, b, r);
}

ss::future<std::vector<int32_t>>
compaction_filter::compute_offset_deltas_to_keep(
  const model::record_batch& b) const {
    std::vector<int32_t> offset_deltas;
    offset_deltas.reserve(b.record_count());
    b.for_each_record([&](const model::record& r) {
        // Note: for_each_record is synchronous. We need the async version.
        // This will be replaced with the async iteration pattern.
    });
    // Use async iteration over records
    auto it = model::record_batch_iterator::create(b);
    while (it.has_next()) {
        auto r = it.next();
        if (co_await should_keep(b, r)) {
            offset_deltas.push_back(r.offset_delta());
        }
    }
    co_return offset_deltas;
}

ss::future<std::optional<model::record_batch>>
compaction_filter::filter_batch_with_offset_deltas(
  model::record_batch b, std::vector<int32_t> offset_deltas) const {
    if (offset_deltas.empty()) {
        // Create a compaction placeholder batch to maintain offset contiguity.
        // Local storage segments need contiguous offsets for index correctness.
        auto hdr = b.header();
        model::record_batch_header new_hdr{
          .size_bytes = model::packed_record_batch_header_size,
          .base_offset = hdr.base_offset,
          .type = model::record_batch_type::compaction_placeholder,
          .crc = 0,
          .attrs = model::record_batch_attributes(
            hdr.attrs.value()
            | static_cast<int16_t>(
              model::record_batch_attributes::ghost_record_bit)),
          .last_offset_delta = hdr.last_offset_delta,
          .first_timestamp = hdr.first_timestamp,
          .max_timestamp = hdr.max_timestamp,
          .producer_id = hdr.producer_id,
          .producer_epoch = hdr.producer_epoch,
          .base_sequence = hdr.base_sequence,
          .record_count = 0,
          .ctx = model::record_batch_header::context{
            .term = hdr.ctx.term,
            .owner_shard = hdr.ctx.owner_shard,
          },
        };
        auto placeholder = model::record_batch(
          new_hdr, iobuf{}, model::record_batch::tag_ctor_ng{});
        placeholder.header().crc = model::crc_record_batch(placeholder);
        placeholder.header().header_crc = model::internal_header_only_crc(
          placeholder.header());
        co_return placeholder;
    }
    co_return co_await do_filter_batch(std::move(b), std::move(offset_deltas));
}

} // namespace storage::compaction
```

Note: The exact placeholder batch construction should match the existing
pattern in `storage::internal::create_ghost_record_batch()` from
`src/v/storage/segment_utils.cc`. The implementor should verify and align
with that existing function.

- [ ] **Step 5: Build and run tests**

Run:
```bash
bazel build //src/v/storage/compaction:compaction_filter
bazel test //src/v/storage/compaction/tests:compaction_filter_test
```

Expected: Build succeeds. Tests pass (or need test body implementations filled
in — see Step 1 note).

- [ ] **Step 6: Commit**

```bash
git add src/v/storage/compaction/compaction_filter.h \
  src/v/storage/compaction/compaction_filter.cc \
  src/v/storage/compaction/BUILD \
  src/v/storage/compaction/tests/compaction_filter_test.cc \
  src/v/storage/compaction/tests/BUILD
git commit -m "storage/compaction: add compaction_filter for local dedup

Extends compaction::filter with tombstone removal via
removable_tombstone_ranges and creates placeholder batches for
empty results to maintain offset contiguity in local segments."
```

---

## Task 4: `replace_offset_range()` on `disk_log_impl`

New method that atomically replaces N segments with 1 segment, with crash-safe
recovery for overlapping segments on startup.

**Files:**
- Modify: `src/v/storage/disk_log_impl.h`
- Modify: `src/v/storage/disk_log_impl.cc`

- [ ] **Step 1: Add method declaration to `disk_log_impl.h`**

Add to the public section of `disk_log_impl` (after the existing compaction
methods around line 300):

```cpp
    /// Replaces all segments in [start, end] with a single replacement
    /// segment. start must equal a segment's base_offset and end must equal
    /// a segment's dirty_offset. Crash-safe: swaps first segment atomically,
    /// then removes the rest. Overlapping segments are cleaned up at startup.
    ss::future<> replace_offset_range(
      model::offset start,
      model::offset end,
      ss::lw_shared_ptr<segment> replacement);
```

- [ ] **Step 2: Implement `replace_offset_range()`**

Add to `disk_log_impl.cc`:

```cpp
ss::future<> disk_log_impl::replace_offset_range(
  model::offset start,
  model::offset end,
  ss::lw_shared_ptr<segment> replacement) {
    auto segment_modify_lock = co_await _segment_rewrite_lock.get_units();

    // Find the range of segments to replace.
    auto begin_it = _segs.lower_bound(start);
    vassert(
      begin_it != _segs.end() && (*begin_it)->offsets().get_base_offset() == start,
      "replace_offset_range: start {} does not align with a segment base",
      start);

    auto end_it = begin_it;
    while (end_it != _segs.end()
           && (*end_it)->offsets().get_dirty_offset() <= end) {
        if ((*end_it)->offsets().get_dirty_offset() == end) {
            ++end_it;
            break;
        }
        ++end_it;
    }
    vassert(
      end_it != begin_it,
      "replace_offset_range: no segments found in [{}, {}]",
      start,
      end);

    // Collect segments to remove (all except the first, which gets swapped).
    auto target = *begin_it;
    chunked_vector<ss::lw_shared_ptr<segment>> to_remove;
    for (auto it = std::next(begin_it); it != end_it; ++it) {
        to_remove.push_back(*it);
    }

    // Evict readers for the entire range.
    auto range_lock = co_await _readers_cache->evict_range(start, end);

    // Acquire write locks on all affected segments.
    chunked_vector<ss::rwlock::holder> write_locks;
    write_locks.push_back(co_await target->write_lock());
    for (auto& seg : to_remove) {
        write_locks.push_back(co_await seg->write_lock());
    }

    // Atomic commit: swap the first segment to the replacement.
    co_await target->index().drop_all_data();
    co_await internal::do_swap_data_file_handles(
      replacement->reader().path(), target, _config);
    target->index().swap_index_state(
      std::move(replacement->index()).release_index_state());
    target->force_set_commit_offset_from_index();
    target->advance_generation_id();
    target->cache().clear();

    // Release write locks before cleanup.
    write_locks.clear();

    // Remove redundant segments. If we crash here, startup recovery
    // handles the overlapping offsets.
    for (auto& seg : to_remove) {
        auto it = std::find(_segs.begin(), _segs.end(), seg);
        if (it != _segs.end()) {
            if (!seg->has_appender()) {
                subtract_segment_bytes(seg, seg->size_bytes());
            }
            _segs.erase(it, std::next(it));
            co_await remove_segment_permanently(
              seg, "replace_offset_range");
        }
    }
}
```

Note: The exact API for `do_swap_data_file_handles`, `swap_index_state`,
etc. should be verified against the current codebase — the function
signatures used in `concatenate_and_rebuild_target_segment` in
`segment_utils.cc` are the reference. The implementor should read
`segment_utils.cc:1274-1310` and match the pattern exactly.

- [ ] **Step 3: Add startup overlap recovery**

In `disk_log_impl.cc`, find the startup path where segments are loaded
(the `start()` method or the segment recovery path in `segment_set.cc`).
Add overlap detection after segments are loaded:

The existing `unsafe_do_recover()` in `src/v/storage/segment_set.cc:278-292`
already detects overlapping segments and adds them to the recovery set.
Verify this existing behavior handles our crash recovery case:

- If segment A (the swapped first segment) covers offsets [0, 100] and
  segment B (not yet removed) covers [50, 100], the existing recovery
  code at line 281 checks `prev.dirty_offset >= cur.base_offset` and
  marks the smaller/older one for recovery.

If the existing logic is sufficient, no changes needed. If not, add a
post-recovery pass in `disk_log_impl::start()` that scans for and removes
fully-overlapped segments.

- [ ] **Step 4: Build and verify**

Run:
```bash
bazel build //src/v/storage:storage
```

Expected: Builds successfully.

- [ ] **Step 5: Commit**

```bash
git add src/v/storage/disk_log_impl.h src/v/storage/disk_log_impl.cc
git commit -m "storage: add replace_offset_range to disk_log_impl

Crash-safe replacement of N segments with 1 segment. Swaps the
first segment atomically (rename), then removes the rest. If a
crash occurs between these steps, the existing segment recovery
logic handles overlapping offset ranges at startup."
```

---

## Task 5: Compaction Sink

Implements `sliding_window_reducer::sink` for local storage. Writes to
`segment_appender`, rolls segments on term/size/offset constraints, calls
`replace_offset_range()` to swap segments.

**Files:**
- Create: `src/v/storage/compaction/compaction_sink.h`
- Create: `src/v/storage/compaction/compaction_sink.cc`
- Modify: `src/v/storage/compaction/BUILD`

- [ ] **Step 1: Create `compaction_sink.h`**

Create `src/v/storage/compaction/compaction_sink.h`:

```cpp
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "compaction/reducer.h"
#include "compaction/types.h"
#include "container/offset_interval_set.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "storage/compaction/compaction_state.h"

#include <seastar/core/future.hh>

#include <limits>
#include <optional>

namespace storage {
class disk_log_impl;
class segment;
class segment_appender;
} // namespace storage

namespace storage::compaction {

/// Cleaned range info accumulated during map building pass.
struct cleaned_range {
    model::offset base_offset;
    model::offset last_offset;
    bool has_tombstones;
};

class compaction_sink final
  : public ::compaction::sliding_window_reducer::sink {
public:
    compaction_sink(
      disk_log_impl& log,
      model_offset_interval_set removable_tombstone_ranges,
      ::compaction::compaction_config cfg);
    ~compaction_sink() noexcept override;

    ss::future<bool>
    initialize(::compaction::sliding_window_reducer::source&) final;
    ss::future<> prepare_iteration(kafka::offset) final;
    ss::future<> finish_iteration(kafka::offset, kafka::offset) final;
    ss::future<ss::stop_iteration>
    operator()(model::record_batch, model::compression) final;
    ss::future<> finalize(bool success) final;

    /// Results for the worker to apply to compaction_state.
    const chunked_vector<cleaned_range>& new_cleaned_ranges() const {
        return _new_cleaned_ranges;
    }
    const model_offset_interval_set& removable_tombstone_ranges() const {
        return _removable_tombstone_ranges;
    }
    const model_offset_interval_set& processed_ranges() const {
        return _processed_ranges;
    }

private:
    ss::future<> initialize_appender(model::offset base, model::term_id term);
    ss::future<> flush_and_replace();
    ss::future<> discard_inflight();
    bool should_roll_for_size() const;
    bool should_roll_for_offset_span(model::offset next) const;

    disk_log_impl& _log;
    model_offset_interval_set _removable_tombstone_ranges;
    ::compaction::compaction_config _cfg;

    // Current output segment state
    ss::lw_shared_ptr<segment> _current_segment;
    std::unique_ptr<segment_appender> _appender;
    model::term_id _current_term;
    model::offset _range_start;
    model::offset _range_end;

    // Tracking
    model_offset_interval_set _processed_ranges;
    chunked_vector<cleaned_range> _new_cleaned_ranges;
};

} // namespace storage::compaction
```

- [ ] **Step 2: Create `compaction_sink.cc`**

Create `src/v/storage/compaction/compaction_sink.cc`:

```cpp
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/compaction/compaction_sink.h"

#include "storage/disk_log_impl.h"
#include "storage/segment.h"
#include "storage/segment_appender.h"

#include <limits>

namespace storage::compaction {

compaction_sink::compaction_sink(
  disk_log_impl& log,
  model_offset_interval_set removable_tombstone_ranges,
  ::compaction::compaction_config cfg)
  : _log(log)
  , _removable_tombstone_ranges(std::move(removable_tombstone_ranges))
  , _cfg(std::move(cfg)) {}

compaction_sink::~compaction_sink() noexcept = default;

ss::future<bool> compaction_sink::initialize(
  ::compaction::sliding_window_reducer::source& src) {
    // Move cleaned ranges from source. The source accumulates these during
    // map_building_iteration(). We access them via the concrete source type.
    // The source is a friend or exposes new_cleaned_ranges().
    //
    // Pattern: same as L1 sink.cc initialize() moving _new_cleaned_ranges
    // from the compaction_source.
    //
    // For now, return true to proceed with deduplication.
    co_return true;
}

ss::future<> compaction_sink::prepare_iteration(kafka::offset base_kafka) {
    // Convert from kafka::offset used by the shared interface to
    // model::offset used internally. In local storage these are the same
    // underlying int64_t for non-translated offsets.
    auto base = model::offset(base_kafka());

    // TODO: The shared interface uses kafka::offset. We need to also pass
    // term_id somehow. This will require extending the interface or passing
    // term through a side channel. For now, we track term from segment
    // metadata in the source's deduplication_iteration.
    //
    // For the initial implementation, the source will set the term on the
    // sink directly before calling prepare_iteration.

    if (!_appender) {
        // First segment — create appender.
        // co_await initialize_appender(base, _current_term);
        _range_start = base;
        co_return;
    }

    // Check term change — roll if needed.
    // (Term is set by source before this call.)

    // Otherwise, continue appending.
    co_return;
}

ss::future<> compaction_sink::finish_iteration(
  kafka::offset base_kafka, kafka::offset last_kafka) {
    auto last = model::offset(last_kafka());
    _range_end = last;
    _processed_ranges.insert(
      model::offset(base_kafka()), model::offset(last_kafka()));
    co_return;
}

ss::future<ss::stop_iteration> compaction_sink::operator()(
  model::record_batch b, model::compression) {
    if (should_roll_for_size() || should_roll_for_offset_span(b.base_offset())) {
        co_await flush_and_replace();
        _range_start = b.base_offset();
    }

    if (!_appender) {
        co_await initialize_appender(_range_start, _current_term);
    }

    co_await _appender->append(b);
    co_return ss::stop_iteration::no;
}

ss::future<> compaction_sink::finalize(bool success) {
    if (!success) {
        co_await discard_inflight();
        co_return;
    }
    if (_appender) {
        co_await flush_and_replace();
    }
    co_return;
}

ss::future<> compaction_sink::initialize_appender(
  model::offset base, model::term_id term) {
    // Create a new staging segment via the log manager, similar to how
    // segment_utils.cc creates staging segments for adjacent compaction.
    // The exact mechanism depends on disk_log_impl's segment creation API.
    //
    // _current_segment = co_await _log.make_compaction_segment(base, term);
    // _appender = _current_segment->release_appender(...);
    _current_term = term;
    _range_start = base;
    co_return;
}

ss::future<> compaction_sink::flush_and_replace() {
    if (!_appender) {
        co_return;
    }
    co_await _appender->flush();
    co_await _appender->close();
    _appender.reset();

    // Finalize the segment (build index, etc.)
    // co_await _current_segment->flush_and_close();

    co_await _log.replace_offset_range(
      _range_start, _range_end, _current_segment);
    _current_segment = nullptr;
}

ss::future<> compaction_sink::discard_inflight() {
    if (_appender) {
        co_await _appender->close();
        _appender.reset();
    }
    if (_current_segment) {
        // Remove the staging segment files.
        _current_segment = nullptr;
    }
}

bool compaction_sink::should_roll_for_size() const {
    if (!_appender) {
        return false;
    }
    return _appender->file_byte_offset()
           >= _log.config().max_compacted_segment_size();
}

bool compaction_sink::should_roll_for_offset_span(model::offset next) const {
    if (!_appender) {
        return false;
    }
    auto span = next() - _range_start();
    return span > std::numeric_limits<uint32_t>::max();
}

} // namespace storage::compaction
```

Note: The `initialize_appender` and `flush_and_replace` methods contain
TODO comments where the exact segment creation API needs to be wired.
The implementor should look at `segment_utils.cc:make_segment_appender()`
and `disk_log_impl::new_segment()` to determine the right pattern. The
sink creates a staging segment (temporary file), writes compacted data,
then calls `replace_offset_range()` to atomically install it.

- [ ] **Step 3: Add sink target to BUILD**

Add to `src/v/storage/compaction/BUILD`:

```python
redpanda_cc_library(
    name = "compaction_sink",
    srcs = [
        "compaction_sink.cc",
    ],
    hdrs = [
        "compaction_sink.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":compaction_state",
        "//src/v/compaction:reducer",
        "//src/v/compaction:types",
        "//src/v/container:offset_interval_set",
        "//src/v/model",
        "//src/v/storage:storage",
        "@seastar",
    ],
)
```

- [ ] **Step 4: Build**

Run:
```bash
bazel build //src/v/storage/compaction:compaction_sink
```

Expected: Builds successfully.

- [ ] **Step 5: Commit**

```bash
git add src/v/storage/compaction/compaction_sink.h \
  src/v/storage/compaction/compaction_sink.cc \
  src/v/storage/compaction/BUILD
git commit -m "storage/compaction: add compaction_sink for local segments

Implements sliding_window_reducer::sink that writes deduplicated
data to segment_appender and calls replace_offset_range() to swap
old segments. Rolls on term change, size, and offset span limits."
```

---

## Task 6: Compaction Source

Implements `sliding_window_reducer::source`. Two-pass: reverse map building
over dirty ranges, then forward deduplication iterating per-segment.

**Files:**
- Create: `src/v/storage/compaction/compaction_source.h`
- Create: `src/v/storage/compaction/compaction_source.cc`
- Modify: `src/v/storage/compaction/BUILD`

- [ ] **Step 1: Create `compaction_source.h`**

Create `src/v/storage/compaction/compaction_source.h`:

```cpp
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "compaction/key_offset_map.h"
#include "compaction/reducer.h"
#include "compaction/types.h"
#include "container/offset_interval_set.h"
#include "model/fundamental.h"
#include "storage/compaction/compaction_sink.h"
#include "storage/compaction/compaction_state.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

namespace storage {
class disk_log_impl;
} // namespace storage

namespace storage::compaction {

class compaction_source final
  : public ::compaction::sliding_window_reducer::source {
public:
    compaction_source(
      disk_log_impl& log,
      ::compaction::key_offset_map& map,
      model_offset_interval_set dirty_ranges,
      model_offset_interval_set removable_tombstone_ranges,
      ::compaction::compaction_config cfg);

    ss::future<> initialize() final;
    ss::future<ss::stop_iteration> map_building_iteration() final;
    ss::future<ss::stop_iteration>
    deduplication_iteration(::compaction::sliding_window_reducer::sink&) final;

    /// Cleaned ranges accumulated during map building, moved to sink
    /// during initialize().
    chunked_vector<cleaned_range>& new_cleaned_ranges() {
        return _new_cleaned_ranges;
    }

private:
    bool preempted() const;

    disk_log_impl& _log;
    ::compaction::key_offset_map& _map;
    model_offset_interval_set _dirty_ranges;
    model_offset_interval_set _removable_tombstone_ranges;
    ::compaction::compaction_config _cfg;

    // Dirty range iteration state (reverse for map building)
    using interval_vec
      = chunked_vector<model_offset_interval_set::interval>;
    interval_vec _dirty_range_intervals;
    interval_vec::const_reverse_iterator _dirty_range_it;

    // Segment iteration state (forward for deduplication)
    // Tracks which segment we're currently processing.
    size_t _dedup_segment_idx{0};

    // Accumulated during map building
    chunked_vector<cleaned_range> _new_cleaned_ranges;
};

} // namespace storage::compaction
```

- [ ] **Step 2: Create `compaction_source.cc`**

Create `src/v/storage/compaction/compaction_source.cc`:

```cpp
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/compaction/compaction_source.h"

#include "compaction/key.h"
#include "compaction/key_offset_map.h"
#include "storage/compaction/compaction_filter.h"
#include "storage/disk_log_impl.h"
#include "storage/log_reader.h"
#include "storage/segment.h"

namespace storage::compaction {

namespace {

/// Local map_building_reducer, same pattern as L1's.
/// Iterates records in a batch and indexes key->offset pairs.
class map_building_reducer {
public:
    struct return_t {
        bool map_is_full{false};
        std::optional<model::offset> max_indexed_offset;
        bool range_has_tombstones{false};
    };

    map_building_reducer(
      ::compaction::key_offset_map& map, model::offset start_offset)
      : _map(map)
      , _start_offset(start_offset) {}

    ss::future<ss::stop_iteration>
    operator()(model::record_batch b) {
        for (auto& r : b) {
            auto o = b.base_offset()
                     + model::offset_delta(r.offset_delta());
            if (o < _start_offset) {
                continue;
            }
            if (r.is_tombstone()) {
                _range_has_tombstones = true;
            }
            auto key = ::compaction::compaction_key{
              iobuf_to_bytes(r.key())};
            auto ok = co_await _map.put(key, o);
            if (!ok) {
                _map_is_full = true;
                _max_indexed_offset = o;
                co_return ss::stop_iteration::yes;
            }
            _max_indexed_offset = o;
        }
        co_return ss::stop_iteration::no;
    }

    return_t end_of_stream() {
        return {_map_is_full, _max_indexed_offset, _range_has_tombstones};
    }

private:
    ::compaction::key_offset_map& _map;
    model::offset _start_offset;
    bool _map_is_full{false};
    bool _range_has_tombstones{false};
    std::optional<model::offset> _max_indexed_offset;
};

} // anonymous namespace

compaction_source::compaction_source(
  disk_log_impl& log,
  ::compaction::key_offset_map& map,
  model_offset_interval_set dirty_ranges,
  model_offset_interval_set removable_tombstone_ranges,
  ::compaction::compaction_config cfg)
  : _log(log)
  , _map(map)
  , _dirty_ranges(std::move(dirty_ranges))
  , _removable_tombstone_ranges(std::move(removable_tombstone_ranges))
  , _cfg(std::move(cfg)) {
    _dirty_range_intervals = _dirty_ranges.to_vec();
    _dirty_range_it = _dirty_range_intervals.crend(); // Will be set in initialize
}

ss::future<> compaction_source::initialize() {
    _dirty_range_it = _dirty_range_intervals.crbegin();
    co_return;
}

ss::future<ss::stop_iteration>
compaction_source::map_building_iteration() {
    if (preempted()) {
        co_return ss::stop_iteration::yes;
    }
    if (_dirty_range_it == _dirty_range_intervals.crend()) {
        co_return ss::stop_iteration::yes;
    }

    const auto& dirty_range = *_dirty_range_it;

    // Create a log reader for this dirty range, reading backwards through
    // the segments covering it.
    //
    // We build the map by reading records within [dirty_range.base_offset,
    // dirty_range.last_offset]. The map_building_reducer indexes
    // key->offset pairs.
    storage::log_reader_config reader_cfg(
      dirty_range.base_offset,
      dirty_range.last_offset,
      ss::default_priority_class());
    reader_cfg.skip_batch_cache = true;

    auto reader = co_await _log.make_reader(reader_cfg);
    auto res = co_await std::move(reader).consume(
      map_building_reducer(_map, model::offset{0}),
      model::no_timeout);

    if (res.max_indexed_offset.has_value()) {
        _new_cleaned_ranges.push_back(cleaned_range{
          .base_offset = dirty_range.base_offset,
          .last_offset = res.map_is_full
                           ? *res.max_indexed_offset
                           : dirty_range.last_offset,
          .has_tombstones = res.range_has_tombstones,
        });
    }

    if (res.map_is_full) {
        co_return ss::stop_iteration::yes;
    }

    ++_dirty_range_it;
    co_return ss::stop_iteration::no;
}

ss::future<ss::stop_iteration> compaction_source::deduplication_iteration(
  ::compaction::sliding_window_reducer::sink& sink) {
    if (preempted()) {
        co_return ss::stop_iteration::yes;
    }

    // Iterate segments forward. Each segment is one iteration.
    const auto& segs = _log.segments();
    if (_dedup_segment_idx >= segs.size()) {
        co_return ss::stop_iteration::yes;
    }

    auto seg = segs[_dedup_segment_idx];
    auto seg_base = seg->offsets().get_base_offset();
    auto seg_last = seg->offsets().get_dirty_offset();

    // Skip segments outside the compactable range.
    if (seg_base > _cfg.max_removable_local_log_offset) {
        co_return ss::stop_iteration::yes;
    }

    // Call prepare_iteration with segment base offset.
    co_await sink.prepare_iteration(kafka::offset(seg_base()));

    // Create a reader for this segment's range.
    storage::log_reader_config reader_cfg(
      seg_base, seg_last, ss::default_priority_class());
    reader_cfg.skip_batch_cache = true;

    auto reader = co_await _log.make_reader(reader_cfg);
    auto ntp = _log.config().ntp();

    compaction_filter filter(
      sink,
      _map,
      std::move(ntp),
      _removable_tombstone_ranges);

    co_await std::move(reader).consume(filter, model::no_timeout);

    co_await sink.finish_iteration(
      kafka::offset(seg_base()), kafka::offset(seg_last()));

    ++_dedup_segment_idx;
    co_return ss::stop_iteration::no;
}

bool compaction_source::preempted() const {
    return _cfg.asrc && _cfg.asrc->abort_requested();
}

} // namespace storage::compaction
```

Note: The exact `make_reader` API and `log_reader_config` construction
should be verified. The implementor should check `disk_log_impl.h` for the
`make_reader()` signature and `src/v/storage/log_reader.h` for
`log_reader_config` fields.

- [ ] **Step 3: Add source target to BUILD**

Add to `src/v/storage/compaction/BUILD`:

```python
redpanda_cc_library(
    name = "compaction_source",
    srcs = [
        "compaction_source.cc",
    ],
    hdrs = [
        "compaction_source.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":compaction_filter",
        ":compaction_sink",
        ":compaction_state",
        "//src/v/compaction:key",
        "//src/v/compaction:key_offset_map",
        "//src/v/compaction:reducer",
        "//src/v/compaction:types",
        "//src/v/container:offset_interval_set",
        "//src/v/model",
        "//src/v/storage:storage",
        "@seastar",
    ],
)
```

- [ ] **Step 4: Build**

Run:
```bash
bazel build //src/v/storage/compaction:compaction_source
```

Expected: Builds successfully.

- [ ] **Step 5: Commit**

```bash
git add src/v/storage/compaction/compaction_source.h \
  src/v/storage/compaction/compaction_source.cc \
  src/v/storage/compaction/BUILD
git commit -m "storage/compaction: add compaction_source for local segments

Implements sliding_window_reducer::source with two-pass algorithm:
reverse map building over dirty ranges, then forward deduplication
iterating per-segment through a compaction_filter."
```

---

## Task 7: Compaction Worker

Orchestrator that ties source, sink, and reducer together. Owns the
key_offset_map. Called from `disk_log_impl::do_compact()`.

**Files:**
- Create: `src/v/storage/compaction/compaction_worker.h`
- Create: `src/v/storage/compaction/compaction_worker.cc`
- Modify: `src/v/storage/compaction/BUILD`

- [ ] **Step 1: Create `compaction_worker.h`**

Create `src/v/storage/compaction/compaction_worker.h`:

```cpp
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "compaction/key_offset_map.h"
#include "compaction/types.h"
#include "storage/compaction/compaction_state.h"

#include <seastar/core/future.hh>

#include <memory>

namespace storage {
class disk_log_impl;
} // namespace storage

namespace storage::compaction {

class compaction_worker {
public:
    compaction_worker();

    /// Run one round of compaction on the given log.
    /// Updates state in-place and persists it on success.
    ss::future<> compact(
      disk_log_impl& log,
      compaction_state& state,
      ::compaction::compaction_config cfg);

private:
    ss::future<> initialize_map();
    ss::future<> persist_compaction_state(
      const std::filesystem::path& partition_dir,
      const compaction_state& state);

    std::unique_ptr<::compaction::hash_key_offset_map> _map;
};

} // namespace storage::compaction
```

- [ ] **Step 2: Create `compaction_worker.cc`**

Create `src/v/storage/compaction/compaction_worker.cc`:

```cpp
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/compaction/compaction_worker.h"

#include "compaction/key_offset_map.h"
#include "compaction/reducer.h"
#include "resource_mgmt/memory_groups.h"
#include "serde/serde.h"
#include "storage/compaction/compaction_sink.h"
#include "storage/compaction/compaction_source.h"
#include "storage/disk_log_impl.h"

#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>

namespace storage::compaction {

compaction_worker::compaction_worker() = default;

ss::future<> compaction_worker::compact(
  disk_log_impl& log,
  compaction_state& state,
  ::compaction::compaction_config cfg) {
    auto info = state.get_compaction_info(
      cfg.max_removable_local_log_offset,
      cfg.max_tombstone_remove_offset,
      cfg.tombstone_retention_ms);

    if (info.dirty_ranges.empty()
        && info.removable_tombstone_ranges.empty()) {
        co_return;
    }

    co_await initialize_map();
    co_await _map->reset();

    auto src = std::make_unique<compaction_source>(
      log,
      *_map,
      std::move(info.dirty_ranges),
      std::move(info.removable_tombstone_ranges),
      cfg);
    auto snk = std::make_unique<compaction_sink>(
      log,
      info.removable_tombstone_ranges,
      cfg);

    auto* snk_ptr = snk.get();
    auto* src_ptr = src.get();

    co_await ::compaction::sliding_window_reducer(
      std::move(src), std::move(snk))
      .run();

    // Apply results to state.
    for (const auto& cr : snk_ptr->new_cleaned_ranges()) {
        state.cleaned_ranges.insert(cr.base_offset, cr.last_offset);
        if (cr.has_tombstones) {
            state.add(compaction_state::cleaned_range_with_tombstones{
              .base_offset = cr.base_offset,
              .last_offset = cr.last_offset,
              .cleaned_with_tombstones_at = model::timestamp::now(),
            });
        }
    }

    // Remove tombstone ranges that were processed.
    // (intersection of removable_tombstone_ranges with processed_ranges)
    // This mirrors the L1 get_removed_tombstone_ranges() logic.
    auto& processed = snk_ptr->processed_ranges();
    auto& removable = snk_ptr->removable_tombstone_ranges();
    auto stream = removable.make_stream();
    while (stream.has_next()) {
        auto iv = stream.next();
        if (processed.covers(iv.base_offset, iv.last_offset)) {
            state.erase_contiguous_range_with_tombstones(
              iv.base_offset, iv.last_offset);
        }
    }

    co_await persist_compaction_state(
      log.config().work_directory(), state);
}

ss::future<> compaction_worker::initialize_map() {
    if (_map) {
        co_return;
    }
    auto reserved = memory_groups().compaction_reserved_memory();
    _map = std::make_unique<::compaction::hash_key_offset_map>();
    co_await _map->initialize(reserved);
}

ss::future<> compaction_worker::persist_compaction_state(
  const std::filesystem::path& partition_dir,
  const compaction_state& state) {
    auto buf = serde::to_iobuf(state);
    auto tmp_path = partition_dir / "compaction_state.tmp";
    auto final_path = partition_dir / "compaction_state";

    auto fd = co_await ss::open_file_dma(
      tmp_path.string(),
      ss::open_flags::wo | ss::open_flags::create | ss::open_flags::truncate);
    auto out = co_await ss::make_file_output_stream(fd);
    for (auto& frag : buf) {
        co_await out.write(frag.get(), frag.size());
    }
    co_await out.flush();
    co_await out.close();

    co_await ss::rename_file(tmp_path.string(), final_path.string());
}

} // namespace storage::compaction
```

- [ ] **Step 3: Add worker target to BUILD**

Add to `src/v/storage/compaction/BUILD`:

```python
redpanda_cc_library(
    name = "compaction_worker",
    srcs = [
        "compaction_worker.cc",
    ],
    hdrs = [
        "compaction_worker.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":compaction_sink",
        ":compaction_source",
        ":compaction_state",
        "//src/v/compaction:key_offset_map",
        "//src/v/compaction:reducer",
        "//src/v/compaction:types",
        "//src/v/model",
        "//src/v/resource_mgmt:memory_groups",
        "//src/v/serde",
        "//src/v/storage:storage",
        "@seastar",
    ],
)
```

- [ ] **Step 4: Build**

Run:
```bash
bazel build //src/v/storage/compaction:compaction_worker
```

Expected: Builds successfully.

- [ ] **Step 5: Commit**

```bash
git add src/v/storage/compaction/compaction_worker.h \
  src/v/storage/compaction/compaction_worker.cc \
  src/v/storage/compaction/BUILD
git commit -m "storage/compaction: add compaction_worker orchestrator

Owns the key_offset_map, computes dirty/removable ranges from
compaction_state, runs the sliding_window_reducer, and persists
state after successful compaction."
```

---

## Task 8: Wire `disk_log_impl` to New Worker

Add `compaction_state` member, load on startup, and wire `do_compact()` to
the new compaction_worker.

**Files:**
- Modify: `src/v/storage/disk_log_impl.h`
- Modify: `src/v/storage/disk_log_impl.cc`

- [ ] **Step 1: Add compaction_state member to disk_log_impl.h**

Add include at top of `disk_log_impl.h`:
```cpp
#include "storage/compaction/compaction_state.h"
#include "storage/compaction/compaction_worker.h"
```

Add member after `_last_compaction_window_start_offset` (around line 501):
```cpp
    std::optional<storage::compaction::compaction_state> _compaction_state;
    std::optional<storage::compaction::compaction_worker> _compaction_worker;
```

- [ ] **Step 2: Load compaction state on startup**

In `disk_log_impl.cc`, add a method to load state from disk:

```cpp
ss::future<> disk_log_impl::load_compaction_state() {
    if (!config().is_locally_compacted()) {
        co_return;
    }
    auto path = config().work_directory() / "compaction_state";
    auto exists = co_await ss::file_exists(path.string());
    if (!exists) {
        _compaction_state.emplace();
        co_return;
    }
    auto fd = co_await ss::open_file_dma(path.string(), ss::open_flags::ro);
    auto size = co_await fd.size();
    auto buf = co_await fd.dma_read_bulk<char>(0, size);
    co_await fd.close();

    iobuf iob;
    iob.append(buf.get(), size);
    _compaction_state = serde::from_iobuf<
      storage::compaction::compaction_state>(std::move(iob));
}
```

Call this from `disk_log_impl::start()`, after segment loading:
```cpp
co_await load_compaction_state();
```

- [ ] **Step 3: Wire do_compact to new worker**

In `do_compact()`, add a new branch that uses the compaction_worker when
the new compaction path is enabled. For now, gate this behind a new config
flag or a simple check:

```cpp
// In do_compact(), before the existing strategy selection:
if (_compaction_state.has_value()) {
    if (!_compaction_worker.has_value()) {
        _compaction_worker.emplace();
    }
    co_await _compaction_worker->compact(
      *this, *_compaction_state, compact_cfg);
    co_return;
}
// ... existing code follows for fallback
```

- [ ] **Step 4: Build the full storage target**

Run:
```bash
bazel build //src/v/storage:storage
```

Expected: Builds successfully.

- [ ] **Step 5: Commit**

```bash
git add src/v/storage/disk_log_impl.h src/v/storage/disk_log_impl.cc
git commit -m "storage: wire disk_log_impl to new compaction worker

Load compaction_state from disk on startup. When present, use the
new compaction_worker instead of the legacy compaction paths."
```

---

## Task 9: Integration Testing

End-to-end test that exercises the full new compaction pipeline.

**Files:**
- Create: `src/v/storage/compaction/tests/compaction_e2e_test.cc`
- Modify: `src/v/storage/compaction/tests/BUILD`

- [ ] **Step 1: Write integration test**

Create `src/v/storage/compaction/tests/compaction_e2e_test.cc`:

```cpp
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/compaction/compaction_state.h"
#include "storage/compaction/compaction_worker.h"

// This test should:
// 1. Create a disk_log_impl with compacted topic config
// 2. Write multiple segments with duplicate keys
// 3. Run compaction_worker::compact()
// 4. Verify:
//    - Duplicate keys are removed (only latest value per key remains)
//    - Segment count is reduced
//    - compaction_state is persisted and correct
//    - Reading the log produces correct data
// 5. Write more data, run compaction again
// 6. Verify incremental compaction works (cleaned_ranges updated)
//
// The implementor should use the storage_e2e_test fixture pattern
// from src/v/storage/tests/ as a reference.

#include <gtest/gtest.h>

TEST(StorageCompactionE2E, BasicDeduplication) {
    // TODO: Implement using storage test fixtures
}

TEST(StorageCompactionE2E, IncrementalCompaction) {
    // TODO: Implement using storage test fixtures
}

TEST(StorageCompactionE2E, TombstoneRemoval) {
    // TODO: Implement using storage test fixtures
}

TEST(StorageCompactionE2E, CrashRecoveryOverlappingSegments) {
    // TODO: Simulate crash between swap and removal,
    // verify startup handles overlapping segments
}
```

- [ ] **Step 2: Add test target to BUILD**

Add to `src/v/storage/compaction/tests/BUILD`:

```python
redpanda_cc_gtest(
    name = "compaction_e2e_test",
    timeout = "moderate",
    srcs = [
        "compaction_e2e_test.cc",
    ],
    deps = [
        "//src/v/storage/compaction:compaction_state",
        "//src/v/storage/compaction:compaction_worker",
        "//src/v/storage:storage",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/storage/tests:batch_generators",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
```

- [ ] **Step 3: Build test**

Run:
```bash
bazel build //src/v/storage/compaction/tests:compaction_e2e_test
```

Expected: Builds successfully.

- [ ] **Step 4: Commit**

```bash
git add src/v/storage/compaction/tests/compaction_e2e_test.cc \
  src/v/storage/compaction/tests/BUILD
git commit -m "storage/compaction: add integration test scaffolding

End-to-end test scaffolding for the new compaction pipeline
covering deduplication, tombstone removal, incremental compaction,
and crash recovery."
```
