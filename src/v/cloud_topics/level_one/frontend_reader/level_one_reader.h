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

#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/log_reader_config.h"
#include "model/record_batch_reader.h"
#include "utils/prefix_logger.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_future.hh>

#include <deque>
#include <expected>
#include <memory>
#include <variant>

namespace cloud_topics {

class level_one_reader_probe;

/// Open stream for the current L1 object, held inside the reader
/// between read_some calls within the same object.
struct open_stream {
    l1::object_id oid;
    kafka::offset last_object_offset;
    std::unique_ptr<l1::object_reader> reader;
    // Size of the object extent backing this stream and how many of those
    // bytes have already been streamed out. Used to estimate the remaining
    // runway in the current object when deciding whether to prefetch the next.
    size_t extent_size{0};
    size_t bytes_streamed{0};
};

/*
 * This class implements a record batch reader for level one.
 *
 * The reader is a state machine with the following states:
 * - empty: no metadata is cached, no data is materialized
 * - ready: metadata is available but no data is materialized
 * - materialized: the reader contains materialized batches
 * - end_of_stream: no more data to read
 *
 * The state transitions are:
 *              ┌───┐
 *              │EOS├───┤Terminate│
 *              └───┘
 *                ▲
 *                │
 *              ┌─┴───┐   ┌─────┐
 * │Init├──────►│empty├──►│ready│
 *              └─────┘   └──┬──┘
 *                  ▲        │
 *                  │        ▼
 *                ┌─┴──────────┐
 *                │materialized│
 *                └────────────┘
 *
 * The reader starts in the 'empty' state.
 *
 * In 'empty' state, the reader queries the metastore to find the L1 object
 * containing data at or after the requested offset. When metadata is found,
 * it transitions to 'ready' state. If no data is available, it transitions
 * to 'end_of_stream'.
 *
 * In 'ready' state, the reader reads the object footer to determine what
 * partition data to materialize, then fetches the required ranges from
 * object storage. When batches are materialized, it transitions to
 * 'materialized' state.
 *
 * In 'materialized' state, data can be consumed. When all materialized
 * batches are consumed, the reader transitions back to 'empty' state
 * and queries the metastore for the next L1 object partition that.
 */
class level_one_log_reader_impl : public model::record_batch_reader::impl {
public:
    level_one_log_reader_impl(
      const cloud_topic_log_reader_config& cfg,
      model::ntp ntp,
      model::topic_id_partition tidp,
      l1::metastore* metastore,
      l1::io* io_interface,
      level_one_reader_probe* probe = nullptr);

    bool is_end_of_stream() const final;

    ss::future<model::record_batch_reader::storage_t>
      do_load_slice(model::timeout_clock::time_point) final;

    fmt::iterator format_to(fmt::iterator it) const final;

    std::optional<private_flags> get_flags() const final;

    ss::future<> finally() noexcept final;

    /// Reset the reader for reuse from the cache. The new config's
    /// start_offset must equal next_read_lower_bound().
    void reset_config(const cloud_topic_log_reader_config& cfg);

    /// The next offset this reader will produce data from.
    kafka::offset next_read_lower_bound() const { return _next_offset; }

    /// Whether the reader has state worth preserving in the cache.
    bool is_reusable() const;

    const model::ntp& ntp() const { return _ntp; }
    const model::topic_id_partition& tidp() const { return _tidp; }

private:
    struct prefetch_entry;

    struct object_info {
        l1::object_id oid;
        l1::footer footer;
        kafka::offset last_offset;
    };

    struct materialize_result {
        chunked_circular_buffer<model::record_batch> batches;
        kafka::offset last_object_offset;
    };

    /*
     * Contacts the L1 metastore to retrieve metadata for an L1 object that
     * contains the target offset. Uses a lookahead buffer populated via
     * get_extent_metadata_forwards — when lookahead_objects > 1, multiple
     * objects are fetched at once; otherwise exactly one is fetched.
     */
    ss::future<std::optional<object_info>> lookup_object_for_offset(
      kafka::offset, model::timeout_clock::time_point deadline);

    /*
     * Fills the lookahead buffer by fetching up to num_objects extents
     * from the metastore starting at the given offset.
     */
    ss::future<>
    fill_lookahead_buffer(kafka::offset offset, size_t num_objects);

    /*
     * Consumes the front entry of the lookahead buffer that covers the
     * given offset, discarding any stale entries. Returns nullopt if the
     * buffer is empty or has no applicable entry.
     */
    std::optional<l1::metastore::object_response>
    consume_lookahead_buffer(kafka::offset offset);

    /*
     * Materialize batches from the L1 object starting from the given offset.
     */
    ss::future<materialize_result> materialize_batches_from_object_offset(
      const object_info&,
      kafka::offset,
      model::timeout_clock::time_point deadline);

    /*
     * Return batches from the reader's current position until the next
     * partition or the end of the object is reached. The set of batches
     * returned may further be limited by restrictions (e.g. byte limit)
     * imposed by the reader configuration.
     */
    ss::future<chunked_circular_buffer<model::record_batch>>
    read_batches(l1::object_reader& reader);

    ss::future<l1::footer> read_footer(
      l1::object_id oid,
      size_t footer_pos,
      size_t object_size,
      ss::abort_source& as);

    /*
     * Returns batches starting at next offset. It will continue to advance next
     * offset until batches are read or end-of-stream is reached.
     */
    ss::future<model::record_batch_reader::storage_t>
      read_some(model::timeout_clock::time_point);

    /*
     * Returns true if accepting the given number of bytes would cause the
     * reader to exceed its configured bytes limit.
     */
    bool is_over_limit_with_bytes(size_t size) const;

    ss::future<> close_reader_safe(l1::object_reader&);

    /// Open an object reader positioned at the start of an extent and return
    /// it. Does not touch _current_stream; the caller decides where it lands
    /// (the foreground read path, or the prefetch slot).
    ss::future<std::expected<open_stream, l1::io::errc>> open_reader_at(
      l1::object_id oid,
      kafka::offset last_object_offset,
      size_t extent_position,
      size_t extent_size,
      ss::abort_source& as);

    /// Close _current_stream if present, swallowing exceptions.
    ss::future<> close_current_stream();

    /// Top up the prefetch queue: while the bytes already downloaded-or-
    /// in-flight ahead of the read cursor are below the configured horizon,
    /// launch background downloads of upcoming objects. The number launched
    /// scales inversely with object size — large objects fill the horizon with
    /// one just-in-time prefetch, runs of small objects fan out into many
    /// concurrent downloads. Synchronous: spawns work and returns immediately.
    void maybe_fill_prefetch();

    /// Background task that downloads the footer and data extent for one
    /// upcoming object into the given queue entry.
    ss::future<>
    do_prefetch(prefetch_entry* entry, l1::metastore::object_response next);

    /// Adopt the front prefetched stream as _current_stream when it serves the
    /// reader's next offset; otherwise discard the whole queue. Returns true if
    /// adopted.
    ss::future<bool> try_adopt_prefetch();

    /// Await and tear down every queued prefetch (used on offset mismatch and
    /// shutdown).
    ss::future<> discard_prefetch_queue();

    void set_end_of_stream();
    bool _end_of_stream{false};

    cloud_topic_log_reader_config _config;
    model::ntp _ntp;
    model::topic_id_partition _tidp;
    kafka::offset _next_offset;
    l1::metastore* _metastore;
    l1::io* _io;
    level_one_reader_probe* _probe;
    prefix_logger _log;
    size_t _bytes_consumed{0};
    bool _was_cached{false};

    // Open stream for the current object. Non-null while the reader is
    // positioned within an object; null before the first read, when
    // transitioning between objects, and in end-of-stream state.
    std::optional<open_stream> _current_stream;

    // Lookahead buffer of object metadata, ordered by ascending offset.
    // Consumed front-to-back as the reader advances through objects.
    // Populated with 1 entry (no prefetch) or N entries (prefetch).
    std::deque<l1::metastore::object_response> _lookahead_buffer;

    // ---- Background prefetch of upcoming objects (see maybe_fill_prefetch) --
    //
    // A queue of in-flight/ready prefetches, ordered by ascending serves_offset
    // and contiguous with the reader's forward progress. Each entry has its own
    // background fiber, all running under _prefetch_gate and cancelled via
    // _prefetch_as (NOT the per-fetch abort source in _config, which may be
    // gone while the reader sits idle in the l1_reader_cache between fetches).
    // The queue depth scales with horizon / object_size: ~1 for large objects,
    // many for runs of small objects, which is what lets prefetch beat the
    // single-object-ahead (2x) ceiling as boundaries get more frequent. Total
    // bytes in flight are bounded by the horizon.
    struct prefetch_entry {
        // The reader's next offset that this prefetch is positioned to serve.
        kafka::offset serves_offset;
        // Last offset in the prefetched object; used to chain the next entry's
        // serves_offset and to advance _current_stream once adopted.
        kafka::offset last_object_offset;
        // Full size of the prefetched object, for horizon accounting.
        size_t object_size{0};
        // Resolved by the fiber once stream/failed is populated.
        ss::shared_promise<> ready;
        // Populated on success; empty if the prefetch failed or found no data.
        std::optional<open_stream> stream;
        bool failed{false};
    };

    // Hard cap on concurrent prefetches, independent of the horizon, to bound
    // connection and memory use for pathologically small objects.
    static constexpr size_t max_concurrent_prefetch = 10;

    size_t _prefetch_horizon_bytes{0};
    ss::gate _prefetch_gate;
    ss::abort_source _prefetch_as;
    std::deque<std::unique_ptr<prefetch_entry>> _prefetch;
};

} // namespace cloud_topics
