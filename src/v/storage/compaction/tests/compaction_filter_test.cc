// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compaction/key.h"
#include "compaction/key_offset_map.h"
#include "compaction/reducer.h"
#include "compaction/utils.h"
#include "container/chunked_circular_buffer.h"
#include "model/batch_builder.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "storage/compaction/compaction_filter.h"
#include "storage/compaction/compaction_state.h"
#include "storage/tests/batch_generators.h"
#include "storage/types.h"

#include <gtest/gtest.h>

namespace {

const auto test_ntp = model::ntp(
  model::ns("kafka"), model::topic("tapioca"), model::partition_id(0));

/// A simple sink that collects filtered batches.
class collecting_sink : public compaction::sliding_window_reducer::sink {
public:
    explicit collecting_sink(
      chunked_circular_buffer<model::record_batch>& output)
      : _output(output) {}

    ss::future<bool>
    initialize(compaction::sliding_window_reducer::source&) final {
        co_return true;
    }

    ss::future<ss::stop_iteration>
    operator()(model::record_batch b, model::compression) final {
        _output.push_back(std::move(b));
        co_return ss::stop_iteration::no;
    }

    ss::future<> finalize(bool) final { co_return; }
    ss::future<> prepare_iteration(kafka::offset) final { co_return; }
    ss::future<> finish_iteration(kafka::offset, kafka::offset) final {
        co_return;
    }

private:
    chunked_circular_buffer<model::record_batch>& _output;
};

/// Helper: populate a key_offset_map from batches (latest offset wins).
ss::future<> index_batches(
  compaction::simple_key_offset_map& map,
  const chunked_circular_buffer<model::record_batch>& batches) {
    for (const auto& b : batches) {
        co_await b.for_each_record_async(
          [&map, &b](this auto, const model::record& r) -> ss::future<> {
              auto key = compaction::compaction_key{iobuf_to_bytes(r.key())};
              auto o = b.base_offset() + model::offset_delta(r.offset_delta());
              co_await map.put(key, o);
          });
    }
}

/// Helper: drive all batches through a compaction_filter via operator().
ss::future<> run_filter(
  storage::local_compaction::compaction_filter& f,
  chunked_circular_buffer<model::record_batch> batches) {
    for (auto& b : batches) {
        co_await f(std::move(b));
    }
}

/// Helper: create a started stm_hookset with no STMs attached.
/// With no transactional STM, is_batch_in_idempotent_window() returns false.
ss::lw_shared_ptr<storage::stm_hookset> make_stm_hookset() {
    auto mgr = ss::make_lw_shared<storage::stm_hookset>();
    mgr->start();
    return mgr;
}

struct filter_test_ctx {
    compaction::simple_key_offset_map map;
    chunked_circular_buffer<model::record_batch> output;
    storage::local_compaction::model_offset_interval_set
      removable_tombstone_ranges;
    storage::local_compaction::model_offset_interval_set
      removable_transaction_ranges;
    ss::lw_shared_ptr<storage::stm_hookset> stm_mgr = make_stm_hookset();

    /// Build and return a compaction_filter. The caller must keep this context
    /// alive for the filter's lifetime.
    storage::local_compaction::compaction_filter
    make_filter(model::offset segment_last_offset = model::offset::max()) {
        return storage::local_compaction::compaction_filter(
          sink,
          map,
          test_ntp,
          segment_last_offset,
          removable_tombstone_ranges,
          removable_transaction_ranges,
          stm_mgr);
    }

    collecting_sink sink{output};

    ~filter_test_ctx() { stm_mgr->stop(); }
};

} // namespace

// Records with duplicate keys across batches are deduplicated: only the latest
// record for each key is kept.
TEST(StorageCompactionFilterTest, DeduplicatesRecordsAcrossBatches) {
    // linear_int_kv_batch_generator produces batches where each batch has
    // `count` records all sharing the same key (the batch index). So across
    // multiple batches we get duplicate keys that should be deduped.
    auto gen = linear_int_kv_batch_generator();
    auto spec = model::test::record_batch_spec{
      .allow_compression = false, .count = 5};
    auto batches = gen(spec, 4);

    filter_test_ctx ctx;
    index_batches(ctx.map, batches).get();
    auto f = ctx.make_filter();
    run_filter(f, std::move(batches)).get();

    // After dedup, each batch should contain exactly 1 record (the latest for
    // each unique key).
    ASSERT_EQ(ctx.output.size(), 4);
    for (auto& b : ctx.output) {
        EXPECT_EQ(b.record_count(), 1);
    }
}

// Removable control batches (tx_fence, group_fence_tx, etc.) are filtered out
// because should_keep returns false for all their records.
TEST(StorageCompactionFilterTest, RemovableControlBatchesAreFilteredOut) {
    chunked_circular_buffer<model::record_batch> batches;
    auto gen = linear_int_kv_batch_generator();
    using type = model::record_batch_type;
    static const auto removable_control_batch_types = {
      type::tx_fence,
      type::group_fence_tx,
      type::group_prepare_tx,
      type::group_abort_tx,
      type::group_commit_tx};
    for (const auto& bt : removable_control_batch_types) {
        auto spec = model::test::record_batch_spec{
          .allow_compression = false, .count = 1, .bt = bt};
        auto bs = gen(spec, 2);
        for (auto& b : bs) {
            batches.push_back(std::move(b));
        }
    }

    filter_test_ctx ctx;
    index_batches(ctx.map, batches).get();
    auto f = ctx.make_filter();
    run_filter(f, std::move(batches)).get();

    // All removable control batches should be filtered out.
    EXPECT_EQ(ctx.output.size(), 0);
}

// Non-filterable batch types (e.g. raft_configuration which shifts offset
// translation) are passed through unconditionally.
TEST(StorageCompactionFilterTest, NonFilterableBatchesPassThrough) {
    auto gen = linear_int_kv_batch_generator();
    auto spec = model::test::record_batch_spec{
      .allow_compression = false,
      .count = 3,
      .bt = model::record_batch_type::raft_configuration};
    auto batches = gen(spec, 2);

    filter_test_ctx ctx;
    // Don't index these in the map - they should pass through regardless.
    auto f = ctx.make_filter();
    run_filter(f, std::move(batches)).get();

    ASSERT_EQ(ctx.output.size(), 2);
    for (auto& b : ctx.output) {
        EXPECT_EQ(b.record_count(), 3);
    }
}

// When all records in a batch are filtered out and the batch is the last in a
// segment, a placeholder batch is emitted to preserve offset contiguity.
TEST(StorageCompactionFilterTest, PlaceholderCreatedForLastBatchInSegment) {
    auto gen = linear_int_kv_batch_generator();
    auto spec = model::test::record_batch_spec{
      .allow_compression = false, .count = 3};

    // Two batches with the same key index → batch 0 is a full duplicate of
    // batch 1.
    gen._idx = 0;
    auto batch0 = gen.make_batch(spec, 0);
    gen._idx = 0;
    auto batch1 = gen.make_batch(
      model::test::record_batch_spec{
        .offset = batch0.last_offset() + model::offset(1),
        .allow_compression = false,
        .count = 3},
      0);

    // The segment_last_offset is batch0's last offset, making batch0 the "last
    // batch in segment".
    auto segment_last_offset = batch0.last_offset();

    chunked_circular_buffer<model::record_batch> batches;
    batches.push_back(std::move(batch0));
    batches.push_back(std::move(batch1));

    filter_test_ctx ctx;
    // Index both batches. Because batch1 has higher offsets, batch0's records
    // are all superseded.
    index_batches(ctx.map, batches).get();

    auto f = ctx.make_filter(segment_last_offset);
    run_filter(f, std::move(batches)).get();

    // batch0: all records deduped away, but it's the last in segment →
    // placeholder. batch1: all records share the same key, only the one with
    // the latest offset survives.
    ASSERT_EQ(ctx.output.size(), 2);
    EXPECT_EQ(
      ctx.output[0].header().type,
      model::record_batch_type::compaction_placeholder);
    EXPECT_EQ(ctx.output[0].record_count(), 0);
    EXPECT_EQ(ctx.output[1].record_count(), 1);
}

// A compaction_placeholder batch that is NOT the last in a segment and is NOT
// in an idempotent window is removed entirely.
TEST(StorageCompactionFilterTest, PlaceholderBatchRemovedIfNotLastInSegment) {
    // Manually create a placeholder batch.
    auto gen = linear_int_kv_batch_generator();
    auto spec = model::test::record_batch_spec{
      .allow_compression = false, .count = 1};
    auto original = gen.make_batch(spec, 0);
    auto placeholder_hdr = original.header();
    auto placeholder = compaction::make_placeholder_batch(placeholder_hdr);

    // Also create a normal batch after the placeholder.
    auto normal = gen.make_batch(
      model::test::record_batch_spec{
        .offset = placeholder.last_offset() + model::offset(1),
        .allow_compression = false,
        .count = 1},
      1);

    auto segment_last_offset = normal.last_offset();

    chunked_circular_buffer<model::record_batch> batches;
    batches.push_back(placeholder.copy());
    batches.push_back(normal.copy());

    filter_test_ctx ctx;
    index_batches(ctx.map, batches).get();
    auto f = ctx.make_filter(segment_last_offset);
    run_filter(f, std::move(batches)).get();

    // The placeholder is not the last batch → removed.
    // The normal batch survives.
    ASSERT_EQ(ctx.output.size(), 1);
    EXPECT_EQ(ctx.output[0].header().type, model::record_batch_type::raft_data);
}

// A compaction_placeholder batch that IS the last in a segment survives.
TEST(StorageCompactionFilterTest, PlaceholderBatchKeptIfLastInSegment) {
    auto gen = linear_int_kv_batch_generator();
    auto spec = model::test::record_batch_spec{
      .allow_compression = false, .count = 1};
    auto normal = gen.make_batch(spec, 0);
    auto placeholder_hdr_spec = model::test::record_batch_spec{
      .offset = normal.last_offset() + model::offset(1),
      .allow_compression = false,
      .count = 1};
    auto original = gen.make_batch(placeholder_hdr_spec, 1);
    auto hdr = original.header();
    auto placeholder = compaction::make_placeholder_batch(hdr);

    auto segment_last_offset = placeholder.last_offset();

    chunked_circular_buffer<model::record_batch> batches;
    batches.push_back(normal.copy());
    batches.push_back(placeholder.copy());

    filter_test_ctx ctx;
    index_batches(ctx.map, batches).get();
    auto f = ctx.make_filter(segment_last_offset);
    run_filter(f, std::move(batches)).get();

    // Both batches survive: normal batch has unique keys, placeholder is last.
    ASSERT_EQ(ctx.output.size(), 2);
    EXPECT_EQ(
      ctx.output[1].header().type,
      model::record_batch_type::compaction_placeholder);
}

// Committed transactional raft_data batches have their transactional bit
// removed during compaction.
TEST(StorageCompactionFilterTest, TransactionalBitUnsetForCommittedRaftData) {
    auto gen = linear_int_kv_batch_generator();
    auto spec = model::test::record_batch_spec{
      .allow_compression = false,
      .count = 3,
      .is_transactional = true,
    };
    auto batches = gen(spec, 1);
    // Verify the batch is transactional and not control.
    ASSERT_TRUE(batches[0].header().attrs.is_transactional());
    ASSERT_FALSE(batches[0].header().attrs.is_control());

    filter_test_ctx ctx;
    index_batches(ctx.map, batches).get();
    auto f = ctx.make_filter();
    run_filter(f, std::move(batches)).get();

    ASSERT_EQ(ctx.output.size(), 1);
    // Transactional bit should be removed for committed raft_data.
    EXPECT_FALSE(ctx.output[0].header().attrs.is_transactional());
}

// Control batch records in removable_transaction_ranges are discarded.
TEST(StorageCompactionFilterTest, ControlBatchInTransactionRangeDiscarded) {
    auto gen = linear_int_kv_batch_generator();
    auto spec = model::test::record_batch_spec{
      .allow_compression = false,
      .count = 1,
      .is_transactional = true,
      .is_control = true,
    };
    auto batches = gen(spec, 1);
    // make_batch doesn't set the control bit from the spec, so set it manually.
    batches[0].header().attrs.set_control_type();
    batches[0].header().reset_size_checksum_metadata(batches[0].data());
    ASSERT_TRUE(batches[0].header().attrs.is_control());

    auto base = batches[0].base_offset();
    auto last = batches[0].last_offset();

    filter_test_ctx ctx;
    ctx.removable_transaction_ranges.insert(base, last);
    index_batches(ctx.map, batches).get();
    auto f = ctx.make_filter();
    run_filter(f, std::move(batches)).get();

    // The control batch's record was in the removable transaction range, so the
    // entire batch should be filtered out (no records kept → batch discarded).
    EXPECT_EQ(ctx.output.size(), 0);
}

// When all records are deduped away from a non-last-in-segment batch, it is
// fully discarded (no placeholder emitted).
TEST(StorageCompactionFilterTest, FullyDedupedNonLastBatchIsDiscarded) {
    auto gen = linear_int_kv_batch_generator();
    auto spec = model::test::record_batch_spec{
      .allow_compression = false, .count = 3};

    // Two batches with the same keys.
    gen._idx = 0;
    auto batch0 = gen.make_batch(spec, 0);
    gen._idx = 0;
    auto batch1 = gen.make_batch(
      model::test::record_batch_spec{
        .offset = batch0.last_offset() + model::offset(1),
        .allow_compression = false,
        .count = 3},
      0);

    // segment_last_offset beyond both batches — neither is last.
    auto segment_last_offset = model::offset::max();

    chunked_circular_buffer<model::record_batch> batches;
    batches.push_back(std::move(batch0));
    batches.push_back(std::move(batch1));

    filter_test_ctx ctx;
    index_batches(ctx.map, batches).get();
    auto f = ctx.make_filter(segment_last_offset);
    run_filter(f, std::move(batches)).get();

    // batch0 is fully deduped and not last → discarded entirely.
    // batch1: all records share the same key, only the latest survives.
    ASSERT_EQ(ctx.output.size(), 1);
    EXPECT_EQ(ctx.output[0].record_count(), 1);
}

// A control batch whose offsets are NOT in removable_transaction_ranges is
// kept.
TEST(StorageCompactionFilterTest, ControlBatchNotInTransactionRangeIsKept) {
    auto gen = linear_int_kv_batch_generator();
    auto spec = model::test::record_batch_spec{
      .allow_compression = false,
      .count = 1,
      .is_transactional = true,
      .is_control = true,
    };
    auto batches = gen(spec, 1);
    batches[0].header().attrs.set_control_type();
    batches[0].header().reset_size_checksum_metadata(batches[0].data());
    ASSERT_TRUE(batches[0].header().attrs.is_control());

    // Leave removable_transaction_ranges empty — the batch is not in range.
    filter_test_ctx ctx;
    index_batches(ctx.map, batches).get();
    auto f = ctx.make_filter();
    run_filter(f, std::move(batches)).get();

    // Control batch not in the removable range is kept.
    ASSERT_EQ(ctx.output.size(), 1);
    EXPECT_TRUE(ctx.output[0].header().attrs.is_control());
}

// Helper: build a batch containing a single tombstone record (no value) at the
// given offset with the given key.
model::record_batch make_tombstone_batch(model::offset base, iobuf key) {
    model::batch_builder builder;
    builder.set_base_offset(base);
    model::record rec(
      model::record_attributes{},
      /*timestamp_delta=*/0,
      /*offset_delta=*/0,
      std::make_optional(std::move(key)),
      /*value=*/std::nullopt,
      chunked_vector<model::record_header>{});
    builder.add_record(std::move(rec));
    return builder.build_sync();
}

// Tombstone records in the removable_tombstone_ranges are discarded.
TEST(StorageCompactionFilterTest, TombstoneInRemovableRangeIsDiscarded) {
    auto key_buf = iobuf();
    key_buf.append("tombstone-key", 13);
    auto batch = make_tombstone_batch(model::offset(0), key_buf.copy());

    chunked_circular_buffer<model::record_batch> batches;
    batches.push_back(std::move(batch));

    filter_test_ctx ctx;
    ctx.removable_tombstone_ranges.insert(model::offset(0), model::offset(0));
    index_batches(ctx.map, batches).get();
    auto f = ctx.make_filter();
    run_filter(f, std::move(batches)).get();

    // The tombstone record was in the removable range → batch fully filtered
    // out.
    EXPECT_EQ(ctx.output.size(), 0);
}

// Tombstone records NOT in the removable_tombstone_ranges are kept.
TEST(StorageCompactionFilterTest, TombstoneNotInRemovableRangeIsKept) {
    auto key_buf = iobuf();
    key_buf.append("tombstone-key", 13);
    auto batch = make_tombstone_batch(model::offset(0), key_buf.copy());

    chunked_circular_buffer<model::record_batch> batches;
    batches.push_back(std::move(batch));

    // Leave removable_tombstone_ranges empty.
    filter_test_ctx ctx;
    index_batches(ctx.map, batches).get();
    auto f = ctx.make_filter();
    run_filter(f, std::move(batches)).get();

    // Tombstone not in range → kept.
    ASSERT_EQ(ctx.output.size(), 1);
    EXPECT_EQ(ctx.output[0].record_count(), 1);
}
