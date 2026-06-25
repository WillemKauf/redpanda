/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/fake_io.h"
#include "cloud_topics/level_one/frontend_reader/level_one_reader.h"
#include "cloud_topics/level_one/frontend_reader/tests/l1_reader_fixture.h"
#include "cloud_topics/log_reader_config.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "model/timeout_clock.h"

#include <seastar/core/sleep.hh>

#include <array>
#include <chrono>
#include <vector>

// This is a microbenchmark, not a correctness test. It demonstrates the
// behaviour of L1 reader prefetch (cloud_topics_l1_reader_prefetch_bytes):
//
//   * When L1 objects are SMALL, a sequential reader crosses object boundaries
//     often. Each cold boundary pays two serial "S3" round trips (footer +
//     data). Prefetch overlaps the next object's download with the time the
//     consumer spends digesting the current object, hiding those stalls.
//
//   * When L1 objects are LARGE, boundaries are rare relative to the data
//     volume, so there is little for prefetch to hide: prefetch-on and
//     prefetch-off run within noise of each other (the optimization no-ops).
//
// fake_io is in-memory, so we inject:
//   * a per-object-read latency (delayed_io) to model the S3 GET round trip,
//   * a per-batch "think time" in the consumer to model the consumer digesting
//     data / the gap between fetch requests, which is the window prefetch
//     overlaps the next download against.

using namespace std::chrono_literals;

namespace cloud_topics::l1 {

namespace {

// Wraps a fake_io and adds a fixed latency to every object read, modelling the
// round-trip cost of an object-storage GET. read_object_as_iobuf (used for
// footer reads) inherits the base implementation, which calls read_object, so
// footer reads are delayed too.
class delayed_io : public io {
public:
    delayed_io(io& inner, std::chrono::microseconds delay)
      : _inner(inner)
      , _delay(delay) {}

    ss::future<std::expected<ss::input_stream<char>, errc>> read_object(
      object_extent e, ss::abort_source* as, cloud_io::group_id g) override {
        if (_delay.count() > 0) {
            co_await ss::sleep(_delay);
        }
        co_return co_await _inner.read_object(e, as, g);
    }

    ss::future<std::expected<std::unique_ptr<staging_file>, errc>>
    create_tmp_file() override {
        return _inner.create_tmp_file();
    }
    ss::future<std::expected<void, errc>>
    put_object(object_id id, staging_file* f, ss::abort_source* as) override {
        return _inner.put_object(id, f, as);
    }
    ss::future<std::expected<void, errc>> delete_objects(
      chunked_vector<object_id> ids, ss::abort_source* as) override {
        return _inner.delete_objects(std::move(ids), as);
    }
    ss::future<std::expected<cloud_storage_clients::multipart_upload_ref, errc>>
    create_multipart_upload(
      object_id id, size_t part_size, ss::abort_source* as) override {
        return _inner.create_multipart_upload(id, part_size, as);
    }

private:
    io& _inner;
    std::chrono::microseconds _delay;
};

// Consumer that sleeps per batch to model the consumer digesting data. The
// total sleep across a run is total_batches * think, independent of how the
// data is split into objects, so different object layouts are compared at
// equal "consume cost".
struct sleepy_consumer {
    explicit sleepy_consumer(std::chrono::microseconds think)
      : _think(think) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch) {
        ++_batches;
        if (_think.count() > 0) {
            co_await ss::sleep(_think);
        }
        co_return ss::stop_iteration::no;
    }

    size_t end_of_stream() const { return _batches; }

    std::chrono::microseconds _think;
    size_t _batches{0};
};

} // namespace

class prefetch_bench : public l1_reader_fixture {
protected:
    static constexpr auto download_delay = 3ms;
    static constexpr auto per_batch_think = 2ms;

    // Lay out `num_objects` L1 objects for a fresh partition, each holding
    // `batches_per_object` batches at increasing offsets.
    std::pair<model::ntp, model::topic_id_partition>
    build(std::string_view name, int num_objects, int batches_per_object) {
        auto [ntp, tidp] = make_ntidp(name);
        auto next = model::offset{0};
        for (int i = 0; i < num_objects; ++i) {
            auto batches = model::test::make_random_batches(
                             next, batches_per_object)
                             .get();
            next = batches.back().last_offset() + model::offset{1};
            std::vector<tidp_batches_t> tb;
            tb.emplace_back(tidp, std::move(batches));
            make_l1_objects(std::move(tb)).get();
        }
        return {ntp, tidp};
    }

    struct result {
        std::chrono::milliseconds wall;
        size_t batches;
    };

    // Drain a fresh reader for `tidp` end-to-end, returning wall-clock time.
    ss::future<result> run_once(
      const model::ntp& ntp,
      const model::topic_id_partition& tidp,
      bool prefetch) {
        delayed_io dio(_io, download_delay);
        auto cfg = make_test_config();
        // A huge horizon lets the queue fill to its concurrency cap, so runs of
        // small objects fan out into many concurrent downloads. Deep lookahead
        // keeps that depth fed without a metastore RPC on the critical path.
        cfg.prefetch_horizon_bytes = prefetch ? (size_t{1} << 30) : 0;
        cfg.lookahead_objects = prefetch ? 64 : 0;
        auto reader = model::record_batch_reader(
          std::make_unique<level_one_log_reader_impl>(
            cfg, ntp, tidp, &_metastore, &dio));

        auto start = std::chrono::steady_clock::now();
        // finally() (awaited by consume) drains any in-flight prefetch before
        // the reader and dio go out of scope.
        auto batches = co_await std::move(reader).consume(
          sleepy_consumer(per_batch_think), model::no_timeout);
        auto wall = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - start);
        co_return result{wall, batches};
    }
};

TEST_F(prefetch_bench, speedup_scales_as_objects_shrink) {
    // Hold total data constant and vary how it is sliced into L1 objects, from
    // one big object (no boundaries) down to one batch per object (a boundary
    // every batch). Depth-1 prefetch would cap at 2x; the depth-K queue lets
    // the speedup keep climbing as boundaries get more frequent, because more
    // of the per-object download latency is hidden behind concurrent fetches.
    constexpr int total_batches = 32;
    constexpr std::array batches_per_object = {32, 16, 8, 4, 2, 1};

    struct row {
        int objects;
        int bpo;
        result off;
        result on;
    };
    std::vector<row> rows;
    for (int bpo : batches_per_object) {
        const int objects = total_batches / bpo;
        auto layout = build(fmt::format("objs_{}", objects), objects, bpo);
        auto off = run_once(layout.first, layout.second, false).get();
        auto on = run_once(layout.first, layout.second, true).get();
        EXPECT_EQ(off.batches, total_batches);
        EXPECT_EQ(on.batches, total_batches);
        rows.push_back(row{objects, bpo, off, on});
    }

    auto ratio = [](result off, result on) {
        return on.wall.count() == 0
                 ? 0.0
                 : static_cast<double>(off.wall.count()) / on.wall.count();
    };

    fmt::print(
      "\n=== L1 reader prefetch: speedup vs object size ===\n"
      "download_delay={}ms/read, think={}ms/batch, {} batches total\n"
      "{:>8} {:>8} {:>10} {:>10} {:>8}\n",
      download_delay.count(),
      per_batch_think.count(),
      total_batches,
      "objects",
      "bpo",
      "off (ms)",
      "on (ms)",
      "speedup");
    for (const auto& r : rows) {
        fmt::print(
          "{:>8} {:>8} {:>10} {:>10} {:>7.2f}x\n",
          r.objects,
          r.bpo,
          r.off.wall.count(),
          r.on.wall.count(),
          ratio(r.off, r.on));
    }

    const auto& one_object = rows.front();   // 1 object, no boundaries
    const auto& many_objects = rows.back();  // 32 objects, a boundary per batch
    const auto& mid_objects = rows[rows.size() / 2];

    // One object: nothing to prefetch, so on ~= off (no-op).
    EXPECT_LT(ratio(one_object.off, one_object.on), 1.3);
    // Many tiny objects: depth-K prefetch hides enough boundary latency to beat
    // the 2x ceiling that a single-object-ahead prefetch would impose.
    EXPECT_GT(ratio(many_objects.off, many_objects.on), 2.0);
    // And the benefit grows as objects shrink.
    EXPECT_GT(
      ratio(many_objects.off, many_objects.on),
      ratio(mid_objects.off, mid_objects.on));
}

} // namespace cloud_topics::l1
