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

#pragma once

#include "base/seastarx.h"
#include "bytes/bytes.h"
#include "container/chunked_hash_map.h"
#include "model/timestamp.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/timer.hh>

#include <chrono>
#include <deque>
#include <filesystem>
#include <memory>

namespace dedup {

/// Outcome of checking a key against the dedup window.
enum class decision : uint8_t {
    /// The key has not been seen within the window; it was just recorded as
    /// first-seen and should be replicated.
    replicate,
    /// The key was already seen (and is still live) within the window; the
    /// record is a duplicate and should be dropped.
    drop,
};

struct windowed_dedup_map_config {
    /// The dedup window length (maps to the `redpanda.dedup.window.ms` topic
    /// property). A key first seen at time T is considered a duplicate for any
    /// subsequent occurrence with timestamp in [T, T + window).
    std::chrono::milliseconds window{std::chrono::minutes{2}};

    /// When the in-memory table exceeds this size, it is frozen and spilled to
    /// disk as an immutable, sorted, on-disk segment.
    static constexpr size_t default_max_memtable_bytes = 32UL * 1024 * 1024;
    size_t max_memtable_bytes{default_max_memtable_bytes};

    /// Directory under which on-disk segments are written. Files here are
    /// node-local and ephemeral: they are not recovered across process
    /// restarts and may be safely deleted while the map is not running.
    std::filesystem::path data_directory;

    /// How often the idle backstop runs. During active ingest, expired
    /// segments are evicted on the produce path using record time; this timer
    /// only does work when there has been no produce activity for a full
    /// interval, reclaiming stale segments by wall-clock once ingest stops.
    static constexpr std::chrono::seconds default_gc_interval{30};
    std::chrono::milliseconds gc_interval{default_gc_interval};
};

/// A sliding-window key -> first-seen-timestamp map used for window-based
/// record deduplication.
///
/// # Semantics
///
/// For each key, the map remembers the timestamp of the *first* occurrence
/// within the configured window. While that entry is live (i.e. its timestamp
/// is within `window` of the query time) any further occurrence of the key is
/// reported as a duplicate (`decision::drop`). Once the window elapses the
/// entry expires and the next occurrence becomes a new first-seen.
///
/// # Why it spills to disk efficiently
///
/// The map is structured as a *time-ordered, never-compacted* LSM tree:
///
///  - Recent keys live in an in-memory hash table (the "memtable").
///  - When the memtable exceeds `max_memtable_bytes` it is frozen and written
///    as an immutable, key-sorted, block-structured on-disk segment.
///  - Because keys expire purely by time, eviction never requires a merge or
///    compaction: a whole segment is dropped (a single file unlink) once its
///    maximum timestamp falls out of the window. This is the key difference
///    from a general spilling map and makes garbage collection O(1) per
///    segment.
///  - Each on-disk segment keeps an in-memory Bloom filter and a sparse,
///    per-block key index. A lookup consults the memtable, then the Bloom
///    filters; only a positive Bloom hit triggers a single ~block-sized disk
///    read. The common "not a duplicate" path therefore performs zero disk
///    I/O, keeping the replicate hot path fast.
///
/// Total on-disk footprint is bounded by (ingest rate x window), and resident
/// memory by `max_memtable_bytes` plus the (small) per-segment Bloom filter and
/// sparse index.
///
/// # Concurrency
///
/// Designed for the seastar thread-per-core model: a single shard owns one
/// instance and calls into it cooperatively. Deduplication is best-effort for
/// keys whose *first* occurrences race across a suspension point (two
/// concurrent first-time lookups of the same not-yet-recorded key may both be
/// admitted); this cannot produce a false duplicate, only a missed one.
class windowed_dedup_map {
public:
    explicit windowed_dedup_map(windowed_dedup_map_config);
    windowed_dedup_map(const windowed_dedup_map&) = delete;
    windowed_dedup_map& operator=(const windowed_dedup_map&) = delete;
    windowed_dedup_map(windowed_dedup_map&&) = delete;
    windowed_dedup_map& operator=(windowed_dedup_map&&) = delete;
    ~windowed_dedup_map();

    /// Check `key` against the window ending at `now` and atomically record it
    /// as first-seen if it is not a live duplicate.
    ///
    /// May perform a single block-sized disk read on a Bloom filter hit;
    /// otherwise resolves without I/O.
    ss::future<decision> check_and_record(const bytes& key, model::timestamp now);

    /// Advance the frontier to `now` and drop any on-disk segments that have
    /// fallen out of the window. Expired-segment eviction also happens
    /// automatically as records arrive; this is only needed to reclaim space
    /// during an ingest lull, when no records are advancing the frontier.
    ss::future<> gc(model::timestamp now);

    /// Close and remove all on-disk segments and discard in-memory state. The
    /// dedup window is ephemeral, so nothing is persisted across stop(). The
    /// instance must not be used afterwards.
    ss::future<> stop();

    size_t memtable_bytes() const { return _memtable_bytes; }
    size_t segment_count() const { return _segments.size(); }

private:
    // An immutable, key-sorted on-disk segment plus its in-memory metadata.
    class segment;

    ss::future<> maybe_flush();
    ss::future<> flush();
    // Pop fully-expired segments off the head of the time-ordered queue.
    ss::future<> drop_expired_front();
    model::timestamp window_cutoff(model::timestamp now) const;

    windowed_dedup_map_config _cfg;

    // Mutable table accumulating the newest keys.
    using memtable = chunked_hash_map<bytes, model::timestamp>;
    memtable _active;
    size_t _memtable_bytes{0};

    // A memtable that has been frozen but whose on-disk segment is still being
    // written. Consulted by lookups so no data is lost during a flush. At most
    // one flush is ever in flight (the op mutex is held across it).
    std::unique_ptr<memtable> _flushing;

    // On-disk segments as a time-ordered FIFO, oldest (head) -> newest (tail).
    // Expired segments are popped off the head.
    std::deque<std::unique_ptr<segment>> _segments;

    // The largest timestamp ever observed; the right edge of the window. The
    // head of the queue is evicted once it is older than (_frontier - window).
    model::timestamp _frontier{model::timestamp::min()};

    // Idle backstop: evicts stale segments by wall-clock when no records are
    // arriving to advance the frontier. Skips itself whenever the produce path
    // has been active since the last tick, so it never interferes with ingest.
    void on_gc_timer();
    ss::timer<ss::lowres_clock> _gc_timer;
    ss::gate _gc_gate;
    bool _active_since_tick{false};

    uint64_t _next_segment_id{0};
    bool _dir_created{false};
    bool _stopped{false};
    // Serializes operations so that lookups never observe the segment/flushing
    // lists mutate across a suspension point. The all-in-memory fast path holds
    // it without suspending, so it does not throttle in-memory throughput; it
    // is only contended while a (rare) disk read or a flush is in flight.
    ss::semaphore _mutex{1};
};

} // namespace dedup
