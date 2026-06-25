/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "bytes/bytes.h"
#include "dedup/windowed_dedup_map.h"
#include "model/timestamp.h"
#include "test_utils/test.h"

#include <seastar/core/sleep.hh>
#include <seastar/util/tmp_file.hh>

#include <gtest/gtest.h>

#include <chrono>
#include <string_view>

using namespace std::chrono_literals;

namespace {

bytes key(std::string_view s) {
    return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
}

model::timestamp ts(int64_t ms) { return model::timestamp(ms); }

dedup::windowed_dedup_map_config cfg(
  const std::filesystem::path& dir,
  size_t memtable_bytes,
  std::chrono::milliseconds gc_interval = 30s) {
    return dedup::windowed_dedup_map_config{
      .window = 1000ms,
      .max_memtable_bytes = memtable_bytes,
      .data_directory = dir,
      .gc_interval = gc_interval,
    };
}

} // namespace

// First occurrence is replicated; an occurrence within the window is dropped;
// once the window elapses the key is admitted again.
TEST_CORO(windowed_dedup_map, in_memory_window) {
    auto dir = co_await ss::make_tmp_dir("dedup-XXXX");
    dedup::windowed_dedup_map m(cfg(dir.get_path(), /*never flush*/ 1UL << 30));

    EXPECT_EQ(
      co_await m.check_and_record(key("a"), ts(0)), dedup::decision::replicate);
    // Same key, still inside the window -> duplicate.
    EXPECT_EQ(
      co_await m.check_and_record(key("a"), ts(500)), dedup::decision::drop);
    EXPECT_EQ(
      co_await m.check_and_record(key("a"), ts(1000)), dedup::decision::drop);
    // A different key is independent.
    EXPECT_EQ(
      co_await m.check_and_record(key("b"), ts(500)),
      dedup::decision::replicate);
    // The window for "a" (first seen at 0, length 1000) has elapsed.
    EXPECT_EQ(
      co_await m.check_and_record(key("a"), ts(1001)),
      dedup::decision::replicate);

    co_await m.stop();
    co_await dir.remove();
}

// With a tiny memtable budget every key spills to its own on-disk segment, so a
// duplicate must be detected by reading the key back from disk.
TEST_CORO(windowed_dedup_map, duplicate_served_from_disk) {
    auto dir = co_await ss::make_tmp_dir("dedup-XXXX");
    dedup::windowed_dedup_map m(cfg(dir.get_path(), /*flush every insert*/ 1));

    EXPECT_EQ(
      co_await m.check_and_record(key("first"), ts(0)),
      dedup::decision::replicate);
    // Push several more keys; each forces a flush, burying "first" under newer
    // segments.
    for (int i = 0; i < 8; ++i) {
        EXPECT_EQ(
          co_await m.check_and_record(key(fmt::format("k{}", i)), ts(10 + i)),
          dedup::decision::replicate);
    }
    EXPECT_GT(m.segment_count(), 0);

    // "first" is now only on disk, still within its window -> duplicate.
    EXPECT_EQ(
      co_await m.check_and_record(key("first"), ts(900)),
      dedup::decision::drop);
    // A genuinely new key whose Bloom filters all miss takes the no-I/O path.
    EXPECT_EQ(
      co_await m.check_and_record(key("brand-new"), ts(900)),
      dedup::decision::replicate);

    co_await m.stop();
    co_await dir.remove();
}

// Segments whose timestamps fall entirely outside the window are dropped by gc.
TEST_CORO(windowed_dedup_map, gc_drops_expired_segments) {
    auto dir = co_await ss::make_tmp_dir("dedup-XXXX");
    dedup::windowed_dedup_map m(cfg(dir.get_path(), /*flush every insert*/ 1));

    co_await m.check_and_record(key("old"), ts(0));
    EXPECT_GT(m.segment_count(), 0);

    // Well past the window for everything written so far.
    co_await m.gc(ts(5000));
    EXPECT_EQ(m.segment_count(), 0);

    // The expired key is admitted again as a fresh first-seen.
    EXPECT_EQ(
      co_await m.check_and_record(key("old"), ts(5001)),
      dedup::decision::replicate);

    co_await m.stop();
    co_await dir.remove();
}

// With no further records to advance the frontier, the idle timer reclaims
// stale segments on its own (using wall-clock time).
TEST_CORO(windowed_dedup_map, idle_timer_reclaims_segments) {
    auto dir = co_await ss::make_tmp_dir("dedup-XXXX");
    dedup::windowed_dedup_map m(
      cfg(dir.get_path(), /*flush every insert*/ 1, /*gc_interval*/ 50ms));

    // A record stamped in the distant past relative to wall-clock; once the
    // timer advances the frontier to "now" it is well outside the window.
    co_await m.check_and_record(key("stale"), ts(0));
    EXPECT_GT(m.segment_count(), 0);

    // No more produce activity: after a couple of timer intervals the idle
    // backstop should have evicted the segment.
    for (int i = 0; i < 20 && m.segment_count() > 0; ++i) {
        co_await ss::sleep(50ms);
    }
    EXPECT_EQ(m.segment_count(), 0);

    co_await m.stop();
    co_await dir.remove();
}
