# Storage Compaction Rewrite Design

## Overview

Rewrite the storage layer's compaction system to follow the same architecture
as the cloud_topics L1 compaction implementation. The new system uses the shared
`compaction::sliding_window_reducer` interfaces (source, sink, filter) with a
two-pass algorithm (reverse map building, forward deduplication) and replaces
all existing compaction paths (self-compaction, adjacent merge, sliding window)
with a single unified pipeline.

All new code lives in `src/v/storage/compaction/`. Existing compaction code is
not removed.

## Components

### 1. Compaction State (`compaction_state.h/cc`)

A serde-serialized struct persisted as a file in the partition directory.
Mirrors L1's `compaction_state` from `cloud_topics/level_one/metastore/state.h`,
adapted for local storage (uses `model::offset` instead of `kafka::offset`).

```cpp
struct compaction_state
  : serde::envelope<compaction_state, serde::version<0>, serde::compat_version<0>> {

    // Ranges whose keys have been deduplicated from the beginning of the log.
    // Dirty ranges are the complement of this set.
    offset_interval_set cleaned_ranges;

    // Cleaned ranges containing tombstones, tracked for delete.retention.ms.
    struct cleaned_range_with_tombstones
      : serde::envelope<cleaned_range_with_tombstones, serde::version<0>,
                         serde::compat_version<0>> {
        model::offset base_offset;
        model::offset last_offset;
        model::timestamp cleaned_with_tombstones_at;
    };
    absl::btree_set<cleaned_range_with_tombstones> cleaned_ranges_with_tombstones;
};
```

**Helper methods** (same as L1):
- `may_add()`, `add()` for cleaned_range_with_tombstones
- `has_contiguous_range_with_tombstones()`,
  `erase_contiguous_range_with_tombstones()` for tombstone removal
- `truncate_with_new_start_offset()` for prefix truncation
- `get_compaction_info(max_removable_offset, max_tombstone_remove_offset,
  tombstone_retention_ms)` returns `{dirty_ranges, removable_tombstone_ranges}`

**Persistence:**
- File path: `<partition_dir>/compaction_state`
- Written after each successful compaction pass (full rewrite, not append)
- Uses write-to-tmp + rename pattern for crash safety
- Read on `disk_log_impl` startup; missing file = fresh state
- `disk_log_impl` holds a `std::optional<compaction_state>` member

### 2. Compaction Worker (`compaction_worker.h/cc`)

Orchestrator called from `disk_log_impl::do_compact()`. Owns the
`compaction::key_offset_map` (reused across passes).

```cpp
ss::future<> compact(disk_log_impl& log, compaction_state& state,
                     compaction::compaction_config cfg) {
    auto compaction_info = state.get_compaction_info(
        cfg.max_removable_local_log_offset,
        cfg.max_tombstone_remove_offset,
        cfg.tombstone_retention_ms);

    if (compaction_info.dirty_ranges.empty()) co_return;

    co_await _map.reset();

    auto src = std::make_unique<compaction_source>(
        log, _map,
        std::move(compaction_info.dirty_ranges),
        std::move(compaction_info.removable_tombstone_ranges),
        cfg);
    auto snk = std::make_unique<compaction_sink>(
        log,
        compaction_info.removable_tombstone_ranges,
        cfg);

    co_await compaction::sliding_window_reducer(
        std::move(src), std::move(snk)).run();

    state.apply(snk->new_cleaned_ranges(), snk->removed_tombstone_ranges());
    co_await persist_compaction_state(log.partition_dir(), state);
}
```

### 3. Compaction Source (`compaction_source.h/cc`)

Implements `compaction::sliding_window_reducer::source`. Reads local segments
via `model::record_batch_reader` (standard log reader path, no compacted
indices).

**Constructor parameters:**
- Reference to `disk_log_impl` (segment access, log reader creation)
- Reference to `compaction::key_offset_map` (owned by worker)
- `offset_interval_set dirty_ranges`
- `offset_interval_set removable_tombstone_ranges`
- `compaction::compaction_config` (limits, abort source, min.compaction.lag.ms)

**`initialize()`:** No-op.

**`map_building_iteration()`:**
- Iterates dirty ranges in reverse (newest to oldest), same as L1
- For each dirty range, creates a log reader over the segment(s) covering it
- Consumes with `compaction::map_building_reducer` to populate key_offset_map
- Tracks `new_cleaned_ranges` with `has_tombstones` flag
- Returns `stop_iteration::yes` when map is full or all dirty ranges processed

**`deduplication_iteration(sink&)`:**
- Iterates segments forward over the eligible range
- For each segment:
  - Calls `sink.prepare_iteration(segment.offsets().base_offset, segment.term())`
    to give the sink a chance to roll on term change
  - Creates a log reader for the segment
  - Consumes with `compaction_filter` which calls `sink(batch, compression)`
  - Calls `sink.finish_iteration(segment.offsets().base_offset,
    segment.offsets().dirty_offset)`
- Returns `stop_iteration::yes` when all segments processed or preempted

### 4. Compaction Sink (`compaction_sink.h/cc`)

Implements `compaction::sliding_window_reducer::sink`. Writes deduplicated data
to local segments via `segment_appender`.

**Constructor parameters:**
- Reference to `disk_log_impl` (for `replace_offset_range()`, segment creation)
- `offset_interval_set removable_tombstone_ranges` (passed through to filter)
- `compaction::compaction_config` (max_compacted_segment_size, abort source)

**State:**
- `_appender`: current segment_appender being written to
- `_current_segment`: the segment being built
- `_current_term`: raft term of current output segment
- `_range_start` / `_range_end`: input offset range this output segment replaces
- `_new_cleaned_ranges`: moved from source during `initialize()`
- `_processed_ranges`: tracks actually-processed input ranges

**`initialize(source&)`:**
- Moves `new_cleaned_ranges` from source
- Returns `false` if source indexed no dirty ranges

**`prepare_iteration(kafka::offset base, model::term_id term)`:**
- No appender: create new segment + appender, set `_range_start`, `_current_term`
- Term changed (`term != _current_term`): roll -- flush, `replace_offset_range()`,
  start new segment
- Same term: continue appending

**`operator()(record_batch, compression)`:**
- If `_appender->file_size() >= max_compacted_segment_size`: roll
- If `batch.last_offset - _range_start > uint32_t::max`: roll
- Append batch to appender

**`finish_iteration(kafka::offset base, kafka::offset last)`:**
- Update `_range_end = last`
- Update `_processed_ranges`

**`finalize(bool success)`:**
- If `!success`: discard in-flight segment
- If success: flush final segment, call `replace_offset_range()`
- Compute final cleaned ranges and removed tombstone ranges (intersection of
  `_new_cleaned_ranges` with `_processed_ranges`, same as L1)
- Expose results via `new_cleaned_ranges()` and `removed_tombstone_ranges()`
  for the worker to apply to state

**Rolling triggers:**
1. Term change (detected in `prepare_iteration`)
2. Size exceeds `max_compacted_segment_size` (detected in `operator()`)
3. Offset span exceeds `uint32_t::max` (detected in `operator()`)

### 5. Compaction Filter (`compaction_filter.h/cc`)

Extends `compaction::filter` base class. Nearly identical to L1's filter.

**Constructor parameters:**
- `compaction::sliding_window_reducer::sink&` (passed to base)
- `const compaction::key_offset_map&` (dedup lookups)
- `model::ntp` (logging)
- `const offset_interval_set& removable_tombstone_ranges`

**`compute_offset_deltas_to_keep(const record_batch&)`:**
- For each record:
  - If tombstone and offset in `removable_tombstone_ranges`: discard
  - Otherwise: `compaction::is_latest_record_for_key(map, batch, record)`,
    keep only if latest
- Returns vector of offset deltas to keep

**`filter_batch_with_offset_deltas(record_batch, vector<int32_t>)`:**
- Empty offset_deltas: create a compaction placeholder batch (local storage
  requires contiguous offset space for segment indices, unlike L1 which can
  have gaps between extents)
- Otherwise: delegate to `do_filter_batch()`

### 6. `replace_offset_range()` on `disk_log_impl`

```cpp
ss::future<> replace_offset_range(
    model::offset start,
    model::offset end,
    ss::lw_shared_ptr<segment> replacement);
```

Replaces all segments in `[start, end]` with a single replacement segment.

**Preconditions:**
- `start` equals some segment's `base_offset` in `_segs`
- `end` equals some segment's `dirty_offset` in `_segs`

**Write path:**
1. Acquire `_segment_rewrite_lock`
2. Find segment range via `lower_bound(start)` through segment with
   `dirty_offset == end`
3. Validate preconditions
4. Evict readers via `_readers_cache->evict_range(start, end)`
5. Acquire write locks on all affected segments
6. Swap first segment to replacement via `transfer_segment` (rename data file,
   swap index state, advance generation ID) -- this is the atomic commit point
7. Remove segments 2..N permanently (tombstone + async delete)
8. Update dirty/closed byte counters

**Crash recovery (startup):**
If a crash occurs between step 6 and step 7, the replaced first segment
contains all the data and the not-yet-removed trailing segments have
overlapping offset ranges. During `disk_log_impl` startup when loading
segments from disk, detect overlapping offset ranges: if segment A fully
covers the offset range of segment B, remove B. No additional metadata
files or journals needed.

### 7. `offset_interval_set` Relocation

Move `offset_interval_set` from `src/v/cloud_topics/level_one/metastore/` to
`src/v/container/`. Update all existing references in L1 code to point to the
new location.

## File Layout

```
src/v/storage/compaction/
  BUILD
  compaction_state.h
  compaction_state.cc
  compaction_source.h
  compaction_source.cc
  compaction_sink.h
  compaction_sink.cc
  compaction_filter.h
  compaction_filter.cc
  compaction_worker.h
  compaction_worker.cc
  tests/
    BUILD
    compaction_state_test.cc
    compaction_filter_test.cc
    compaction_worker_test.cc
```

## Dependencies

- `//src/v/compaction` -- shared interfaces (reducer, filter, key_offset_map)
- `//src/v/storage` -- disk_log_impl, segment, segment_appender, readers_cache
- `//src/v/container` -- offset_interval_set (relocated)
- `//src/v/model` -- record types, offsets, timestamps
- `//src/v/serde` -- serialization for compaction_state

## Changes to Existing Files

- **`disk_log_impl.h/cc`**: Add `replace_offset_range()` method, hold
  `std::optional<compaction_state>`, startup recovery for overlapping segments,
  wire `do_compact()` to the new worker
- **`offset_interval_set`**: Move from L1 metastore to `src/v/container/`,
  update all existing references
- **L1 compaction**: Update imports for relocated `offset_interval_set`

## What Is NOT Changed

- Existing compaction code (compacted indices, compaction_reducers,
  self-compaction, adjacent merge, sliding window) remains in place
- Transaction batch removal is deferred to future work
- The `compaction::compaction_config` struct is reused as-is
