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

#include "dedup/windowed_dedup_map.h"

#include "base/vassert.h"
#include "bytes/iobuf.h"
#include "container/chunked_vector.h"
#include "hashing/xx.h"
#include "serde/parquet/bloom_filter.h"
#include "ssx/future-util.h"
#include "utils/file_io.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>

#include <algorithm>
#include <cstring>

namespace dedup {

namespace {

// On-disk segments are split into blocks of roughly this size. A point lookup
// reads exactly one block, so this trades read amplification (larger blocks)
// against sparse-index memory (smaller blocks). The block is allowed to exceed
// the target to always hold at least one whole entry.
constexpr size_t block_target_bytes = 16UL * 1024;

// Rough per-entry overhead charged against the memtable budget: the key bytes
// plus the value and hash-table bookkeeping.
constexpr size_t memtable_entry_overhead = 24;

// Fixed-width values are stored in native byte order. Segment files are
// node-local and ephemeral, so endian portability is not required.
template<typename T>
void append_fixed(iobuf& b, T v) {
    static_assert(std::is_trivially_copyable_v<T>);
    b.append(reinterpret_cast<const char*>(&v), sizeof(T));
}

// Bounds-checked cursor over a contiguous block buffer.
class block_reader {
public:
    block_reader(const char* p, size_t n)
      : _p(p)
      , _end(p + n) {}

    template<typename T>
    bool read_fixed(T& out) {
        static_assert(std::is_trivially_copyable_v<T>);
        if (static_cast<size_t>(_end - _p) < sizeof(T)) {
            return false;
        }
        std::memcpy(&out, _p, sizeof(T));
        _p += sizeof(T);
        return true;
    }

    // Returns a view into the underlying buffer; valid as long as it lives.
    bool read_bytes(uint32_t len, std::string_view& out) {
        if (static_cast<size_t>(_end - _p) < len) {
            return false;
        }
        out = std::string_view(_p, len);
        _p += len;
        return true;
    }

private:
    const char* _p;
    const char* _end;
};

uint64_t hash_key(const bytes& key) {
    return xxhash_64(reinterpret_cast<const char*>(key.data()), key.size());
}

} // namespace

// An immutable, key-sorted, block-structured on-disk segment plus the in-memory
// metadata (Bloom filter and per-block sparse index) needed to probe it.
//
// File layout: a concatenation of blocks. Each block is
//   [u32 entry_count] then entry_count x [u32 key_len][key bytes][i64 ts]
// with entries sorted by key, and blocks partitioning the key space in order
// (every key in block i sorts before every key in block i+1). The format is
// node-local and ephemeral; it is intentionally not endian-portable or
// versioned because segments never outlive the process that wrote them.
class windowed_dedup_map::segment {
public:
    struct index_entry {
        bytes first_key;
        uint64_t offset;
        uint32_t length;
    };

    segment(
      std::filesystem::path path,
      ss::file f,
      chunked_vector<index_entry> index,
      serde::parquet::bloom_filter bloom,
      model::timestamp min_ts,
      model::timestamp max_ts)
      : _path(std::move(path))
      , _file(std::move(f))
      , _index(std::move(index))
      , _bloom(std::move(bloom))
      , _min_ts(min_ts)
      , _max_ts(max_ts) {}

    model::timestamp max_ts() const { return _max_ts; }
    model::timestamp min_ts() const { return _min_ts; }

    // Sort `entries` by key, serialize them into blocks, persist to `path`, and
    // return an open, ready-to-probe segment.
    static ss::future<std::unique_ptr<segment>> create(
      std::filesystem::path path,
      chunked_vector<std::pair<bytes, model::timestamp>> entries);

    // Returns the recorded first-seen timestamp for `key`, or nullopt if the
    // key is not present. `key_hash` must be xxhash_64(key). Performs at most
    // one block-sized read, gated by the Bloom filter.
    ss::future<std::optional<model::timestamp>>
    lookup(const bytes& key, uint64_t key_hash) const;

    ss::future<> close() { return _file.close(); }

    ss::future<> close_and_remove() {
        co_await _file.close();
        co_await ss::remove_file(_path.string());
    }

private:
    std::filesystem::path _path;
    ss::file _file;
    chunked_vector<index_entry> _index;
    serde::parquet::bloom_filter _bloom;
    model::timestamp _min_ts;
    model::timestamp _max_ts;
};

ss::future<std::unique_ptr<windowed_dedup_map::segment>>
windowed_dedup_map::segment::create(
  std::filesystem::path path,
  chunked_vector<std::pair<bytes, model::timestamp>> entries) {
    vassert(!entries.empty(), "refusing to write an empty dedup segment");
    std::ranges::sort(
      entries, [](const auto& a, const auto& b) { return a.first < b.first; });

    iobuf file_buf;
    chunked_vector<index_entry> index;
    serde::parquet::bloom_filter bloom(entries.size());
    auto min_ts = model::timestamp::max();
    auto max_ts = model::timestamp::min();

    size_t i = 0;
    while (i < entries.size()) {
        const uint64_t block_off = file_buf.size_bytes();
        bytes first_key = entries[i].first;

        iobuf body;
        uint32_t count = 0;
        while (i < entries.size() && body.size_bytes() < block_target_bytes) {
            const auto& [k, ts] = entries[i];
            append_fixed(body, static_cast<uint32_t>(k.size()));
            body.append(reinterpret_cast<const char*>(k.data()), k.size());
            append_fixed(body, ts.value());
            bloom.insert(hash_key(k));
            min_ts = std::min(min_ts, ts);
            max_ts = std::max(max_ts, ts);
            ++count;
            ++i;
        }

        iobuf block;
        append_fixed(block, count);
        block.append(std::move(body));
        const auto block_len = static_cast<uint32_t>(block.size_bytes());
        file_buf.append(std::move(block));
        index.push_back(index_entry{
          .first_key = std::move(first_key),
          .offset = block_off,
          .length = block_len});
    }

    co_await write_fully(path, std::move(file_buf));
    auto f = co_await ss::open_file_dma(path.string(), ss::open_flags::ro);
    co_return std::make_unique<segment>(
      std::move(path),
      std::move(f),
      std::move(index),
      std::move(bloom),
      min_ts,
      max_ts);
}

ss::future<std::optional<model::timestamp>>
windowed_dedup_map::segment::lookup(
  const bytes& key, uint64_t key_hash) const {
    if (!_bloom.check(key_hash)) {
        co_return std::nullopt;
    }

    // Find the last block whose first key is <= the target key. If the target
    // sorts before the very first block it cannot be present.
    auto it = std::upper_bound(
      _index.begin(),
      _index.end(),
      key,
      [](const bytes& k, const index_entry& e) { return k < e.first_key; });
    if (it == _index.begin()) {
        co_return std::nullopt;
    }
    --it;

    auto stream = ss::make_file_input_stream(_file, it->offset, it->length);
    auto buf = co_await stream.read_exactly(it->length);
    co_await stream.close();

    block_reader r(buf.get(), buf.size());
    uint32_t count = 0;
    if (!r.read_fixed(count)) {
        co_return std::nullopt;
    }
    const std::string_view target(
      reinterpret_cast<const char*>(key.data()), key.size());
    for (uint32_t e = 0; e < count; ++e) {
        uint32_t klen = 0;
        std::string_view k;
        int64_t ts = 0;
        if (
          !r.read_fixed(klen) || !r.read_bytes(klen, k) || !r.read_fixed(ts)) {
            break;
        }
        if (k == target) {
            co_return model::timestamp(ts);
        }
    }
    co_return std::nullopt;
}

windowed_dedup_map::windowed_dedup_map(windowed_dedup_map_config cfg)
  : _cfg(std::move(cfg)) {
    _gc_timer.set_callback([this] { on_gc_timer(); });
    if (_cfg.gc_interval > std::chrono::milliseconds::zero()) {
        _gc_timer.arm_periodic(_cfg.gc_interval);
    }
}

void windowed_dedup_map::on_gc_timer() {
    // If the produce path advanced the frontier since the last tick it is
    // already evicting using record time; leave it alone. Only act once a full
    // interval has elapsed with no activity, i.e. ingest has gone quiet.
    if (std::exchange(_active_since_tick, false)) {
        return;
    }
    ssx::spawn_with_gate(
      _gc_gate, [this] { return gc(model::new_timestamp()); });
}

windowed_dedup_map::~windowed_dedup_map() {
    vassert(
      _stopped || _segments.empty(),
      "windowed_dedup_map destroyed without stop(); {} open segment(s) leaked",
      _segments.size());
}

model::timestamp
windowed_dedup_map::window_cutoff(model::timestamp now) const {
    return now - model::timestamp(_cfg.window.count());
}

ss::future<decision>
windowed_dedup_map::check_and_record(const bytes& key, model::timestamp now) {
    auto units = co_await ss::get_units(_mutex, 1);
    vassert(!_stopped, "check_and_record called after stop()");

    // Advance the frontier and evict anything that just fell out of the window.
    _active_since_tick = true;
    _frontier = std::max(_frontier, now);
    co_await drop_expired_front();

    const auto cutoff = window_cutoff(now);

    if (auto it = _active.find(key); it != _active.end()) {
        if (it->second >= cutoff) {
            co_return decision::drop;
        }
        // Stale: the previous window for this key has elapsed. Refresh it as a
        // fresh first-seen below.
        it->second = now;
        co_return decision::replicate;
    }

    // A memtable that is mid-flush still holds live keys.
    if (_flushing) {
        if (auto it = _flushing->find(key);
            it != _flushing->end() && it->second >= cutoff) {
            co_return decision::drop;
        }
    }

    // Probe on-disk segments newest -> oldest, skipping any that have fully
    // expired. The Bloom filter inside lookup() avoids disk reads for misses.
    const auto key_hash = hash_key(key);
    for (auto i = _segments.rbegin(); i != _segments.rend(); ++i) {
        const auto& seg = **i;
        if (seg.max_ts() < cutoff) {
            continue;
        }
        auto ts = co_await seg.lookup(key, key_hash);
        if (ts.has_value() && *ts >= cutoff) {
            co_return decision::drop;
        }
    }

    _active.emplace(key, now);
    _memtable_bytes += key.size() + memtable_entry_overhead;
    co_await maybe_flush();
    co_return decision::replicate;
}

ss::future<> windowed_dedup_map::maybe_flush() {
    if (_memtable_bytes >= _cfg.max_memtable_bytes) {
        co_await flush();
    }
}

ss::future<> windowed_dedup_map::flush() {
    if (_active.empty()) {
        co_return;
    }
    if (!_dir_created) {
        co_await ss::recursive_touch_directory(_cfg.data_directory.string());
        _dir_created = true;
    }

    // Freeze the active memtable. Concurrent lookups continue to see its keys
    // via _flushing until the segment is durable.
    _flushing = std::make_unique<memtable>(std::exchange(_active, memtable{}));
    _memtable_bytes = 0;

    chunked_vector<std::pair<bytes, model::timestamp>> entries;
    entries.reserve(_flushing->size());
    for (const auto& [k, ts] : *_flushing) {
        entries.emplace_back(k, ts);
    }

    auto path = _cfg.data_directory
                / fmt::format("dedup-{}.seg", _next_segment_id++);
    auto seg = co_await segment::create(std::move(path), std::move(entries));

    // Commit: publish the segment and drop the frozen memtable with no
    // suspension in between, so lookups always see the keys in exactly one of
    // the two places.
    _segments.push_back(std::move(seg));
    _flushing.reset();
}

ss::future<> windowed_dedup_map::drop_expired_front() {
    const auto cutoff = window_cutoff(_frontier);
    while (!_segments.empty() && _segments.front()->max_ts() < cutoff) {
        co_await _segments.front()->close_and_remove();
        _segments.pop_front();
    }
}

ss::future<> windowed_dedup_map::gc(model::timestamp now) {
    auto units = co_await ss::get_units(_mutex, 1);
    if (_stopped) {
        co_return;
    }
    _frontier = std::max(_frontier, now);
    co_await drop_expired_front();
}

ss::future<> windowed_dedup_map::stop() {
    // Stop the timer and drain any in-flight timer gc before taking the mutex:
    // the gc fiber needs the mutex, so holding it here would deadlock.
    _gc_timer.cancel();
    co_await _gc_gate.close();

    auto units = co_await ss::get_units(_mutex, 1);
    if (_stopped) {
        co_return;
    }
    _stopped = true;
    for (auto& seg : _segments) {
        co_await seg->close_and_remove();
    }
    _segments.clear();
    _flushing.reset();
}

} // namespace dedup
