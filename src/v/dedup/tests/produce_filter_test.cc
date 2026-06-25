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
#include "bytes/iobuf.h"
#include "container/chunked_vector.h"
#include "dedup/produce_filter.h"
#include "dedup/windowed_dedup_map.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "storage/record_batch_builder.h"
#include "test_utils/test.h"

#include <seastar/util/tmp_file.hh>

#include <gtest/gtest.h>

#include <chrono>
#include <optional>
#include <string_view>
#include <vector>

using namespace std::chrono_literals;

namespace {

iobuf str_buf(std::string_view s) {
    iobuf b;
    b.append(s.data(), s.size());
    return b;
}

// Build a non-idempotent raft_data batch. Each entry is the optional dedup-id
// to attach as a `redpanda.dedup.id` header (nullopt = no header). All records
// share the same key and value to prove dedup keys off the header, not the key.
model::record_batch
make_batch(const std::vector<std::optional<std::string_view>>& ids) {
    storage::record_batch_builder bld(
      model::record_batch_type::raft_data, model::offset(0));
    bld.set_timestamp(model::timestamp(1000));
    for (const auto& id : ids) {
        chunked_vector<model::record_header> headers;
        if (id.has_value()) {
            headers.push_back(model::record_header(
              std::optional<iobuf>(str_buf(dedup::dedup_id_header_key)),
              std::optional<iobuf>(str_buf(*id))));
        }
        bld.add_raw_kw(
          std::optional<iobuf>(str_buf("key")),
          std::optional<iobuf>(str_buf("value")),
          std::move(headers));
    }
    return std::move(bld).build();
}

// The dedup ids carried by the surviving records, in order.
std::vector<ss::sstring> ids_of(const model::record_batch& b) {
    std::vector<ss::sstring> out;
    b.for_each_record([&out](const model::record& r) {
        for (const auto& h : r.headers()) {
            if (h.key() == dedup::dedup_id_header_key) {
                auto v = iobuf_to_bytes(h.value());
                out.emplace_back(
                  reinterpret_cast<const char*>(v.data()), v.size());
            }
        }
    });
    return out;
}

struct fixture {
    explicit fixture(ss::tmp_dir& dir)
      : map(dedup::windowed_dedup_map_config{
          .window = 60s,
          .max_memtable_bytes = 1UL << 30, // stay in memory
          .data_directory = dir.get_path(),
        }) {}

    dedup::windowed_dedup_map map;
    bool map_requested = false;

    dedup::lazy_map factory() {
        return [this]() -> dedup::windowed_dedup_map& {
            map_requested = true;
            return map;
        };
    }
};

} // namespace

TEST_CORO(produce_filter, drops_duplicate_dedup_ids) {
    auto dir = co_await ss::make_tmp_dir("dedup-XXXX");
    fixture f(dir);

    // ids a, b, a, c -> the second "a" is a duplicate.
    auto res = co_await dedup::dedup_filter_batch(
      make_batch({"a", "b", "a", "c"}), f.factory());

    EXPECT_EQ(res.outcome, dedup::filter_outcome::rewritten);
    EXPECT_EQ(res.records_dropped, 1);
    EXPECT_EQ(res.batch.record_count(), 3);
    EXPECT_EQ(ids_of(res.batch), (std::vector<ss::sstring>{"a", "b", "c"}));

    co_await f.map.stop();
    co_await dir.remove();
}

TEST_CORO(produce_filter, fully_duplicate_batch) {
    auto dir = co_await ss::make_tmp_dir("dedup-XXXX");
    fixture f(dir);

    co_await dedup::dedup_filter_batch(
      make_batch({"a", "b", "c"}), f.factory());
    auto res = co_await dedup::dedup_filter_batch(
      make_batch({"a", "b", "c"}), f.factory());

    EXPECT_EQ(res.outcome, dedup::filter_outcome::fully_duplicate);
    EXPECT_EQ(res.records_dropped, 3);

    co_await f.map.stop();
    co_await dir.remove();
}

TEST_CORO(produce_filter, all_unique_unchanged) {
    auto dir = co_await ss::make_tmp_dir("dedup-XXXX");
    fixture f(dir);

    auto res = co_await dedup::dedup_filter_batch(
      make_batch({"x", "y", "z"}), f.factory());

    EXPECT_EQ(res.outcome, dedup::filter_outcome::unchanged);
    EXPECT_EQ(res.records_dropped, 0);
    EXPECT_EQ(res.batch.record_count(), 3);

    co_await f.map.stop();
    co_await dir.remove();
}

// Records without the dedup-id header are kept; the ones that have it still
// dedup. The same key on every record must not cause dedup.
TEST_CORO(produce_filter, records_without_id_are_kept) {
    auto dir = co_await ss::make_tmp_dir("dedup-XXXX");
    fixture f(dir);

    auto res = co_await dedup::dedup_filter_batch(
      make_batch({std::nullopt, "a", std::nullopt, "a"}), f.factory());

    // Both unkeyed records survive, plus the first "a"; the second "a" drops.
    EXPECT_EQ(res.outcome, dedup::filter_outcome::rewritten);
    EXPECT_EQ(res.records_dropped, 1);
    EXPECT_EQ(res.batch.record_count(), 3);

    co_await f.map.stop();
    co_await dir.remove();
}

// A batch with no dedup-id headers must be a no-op and must NOT allocate the
// map (the factory is never invoked).
TEST_CORO(produce_filter, no_dedup_id_never_allocates) {
    auto dir = co_await ss::make_tmp_dir("dedup-XXXX");
    fixture f(dir);

    auto res = co_await dedup::dedup_filter_batch(
      make_batch({std::nullopt, std::nullopt}), f.factory());

    EXPECT_EQ(res.outcome, dedup::filter_outcome::unchanged);
    EXPECT_EQ(res.batch.record_count(), 2);
    EXPECT_FALSE(f.map_requested) << "map must not be created without a "
                                     "dedup-id record";

    co_await f.map.stop();
    co_await dir.remove();
}

TEST_CORO(produce_filter, idempotent_batch_is_not_touched) {
    auto dir = co_await ss::make_tmp_dir("dedup-XXXX");
    fixture f(dir);

    storage::record_batch_builder bld(
      model::record_batch_type::raft_data, model::offset(0));
    bld.set_producer_identity(/*id*/ 1, /*epoch*/ 0); // makes it idempotent
    bld.set_timestamp(model::timestamp(1000));
    chunked_vector<model::record_header> h1;
    h1.push_back(model::record_header(
      std::optional<iobuf>(str_buf(dedup::dedup_id_header_key)),
      std::optional<iobuf>(str_buf("a"))));
    chunked_vector<model::record_header> h2;
    h2.push_back(model::record_header(
      std::optional<iobuf>(str_buf(dedup::dedup_id_header_key)),
      std::optional<iobuf>(str_buf("a"))));
    bld.add_raw_kw(
      std::optional<iobuf>(str_buf("key")),
      std::optional<iobuf>(str_buf("value")),
      std::move(h1));
    bld.add_raw_kw(
      std::optional<iobuf>(str_buf("key")),
      std::optional<iobuf>(str_buf("value")),
      std::move(h2));

    auto res = co_await dedup::dedup_filter_batch(
      std::move(bld).build(), f.factory());

    EXPECT_EQ(res.outcome, dedup::filter_outcome::unchanged);
    EXPECT_EQ(res.batch.record_count(), 2);
    EXPECT_FALSE(f.map_requested);

    co_await f.map.stop();
    co_await dir.remove();
}
