// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/batch_compression.h"
#include "model/record.h"
#include "model/record_utils.h"
#include "model/tests/random_batch.h"
#include "storage/compacted_index_writer.h"
#include "storage/compaction_reducers.h"
#include "storage/segment_appender.h"
#include "storage/types.h"
#include "test_utils/tmpbuf_file.h"

#include <seastar/core/sleep.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/testing/perf_tests.hh>

namespace {

/// Builds a vector of `num_batches` batches, each with `records_per_batch`
/// records whose keys are each `key_size` bytes. When `compression` is not
/// `none`, each batch is compressed with the specified codec.
std::vector<model::record_batch> make_bench_batches(
  int num_batches,
  int records_per_batch,
  size_t key_size,
  model::compression compression = model::compression::none) {
    std::vector<model::record_batch> batches;
    batches.reserve(num_batches);
    int offset = 0;
    for (int i = 0; i < num_batches; ++i) {
        std::vector<size_t> sizes(records_per_batch, key_size);
        auto batch = model::test::make_random_batch(
          model::test::record_batch_spec{
            .offset = model::offset{offset},
            .allow_compression = false,
            .count = records_per_batch,
            .headers_per_record = 0,
            .record_sizes = std::move(sizes),
          });
        if (compression != model::compression::none) {
            batch = model::compress_batch_sync(compression, std::move(batch));
        }
        batches.push_back(std::move(batch));
        offset += records_per_batch;
    }
    return batches;
}

/// Zero-copy share of all batches.
std::vector<model::record_batch>
share_batches(std::vector<model::record_batch>& batches) {
    std::vector<model::record_batch> ret;
    ret.reserve(batches.size());
    for (auto& b : batches) {
        ret.push_back(b.share());
    }
    return ret;
}

/// Enum controlling which records the filter keeps.
enum class keep_policy {
    all,  // keep every record
    none, // keep no records
    half, // keep every other
};

/// Builds a should-keep filter function for the reducer.
storage::internal::copy_data_segment_reducer::filter_t
make_filter(keep_policy policy) {
    return [policy](
             const model::record_batch_header&,
             const model::record& r,
             bool) -> ss::future<bool> {
        switch (policy) {
        case keep_policy::all:
            co_return true;
        case keep_policy::none:
            co_return false;
        case keep_policy::half:
            co_return (r.offset_delta() % 2) == 0;
        }
    };
}

/// A no-op compacted_index_writer that discards all entries. This allows
/// benchmarking the reducer's indexing path without filesystem I/O.
class noop_compacted_index_writer final
  : public storage::compacted_index_writer {
public:
    noop_compacted_index_writer()
      : compacted_index_writer("bench-noop") {}

    ss::future<>
    index(const compaction::compaction_key&, model::offset, int32_t) final {
        return ss::now();
    }

    ss::future<>
    index(model::record_batch_type, bool, const iobuf&, model::offset, int32_t)
      final {
        return ss::now();
    }

    ss::future<> index(
      model::record_batch_type, bool, bytes&&, model::offset, int32_t) final {
        return ss::now();
    }

    ss::future<> append(storage::compacted_index::entry) final {
        return ss::now();
    }

    ss::future<> close() final { return ss::now(); }
    void set_flag(storage::compacted_index::footer_flags) final {}
    void print(std::ostream& o) const final { o << "noop_cidx"; }
    size_t size_bytes() const final { return 0; }
};

/// Fixture that holds pre-built batches and per-iteration env. The fixture
/// is constructed once per test case (by the perf framework's set_up()),
/// so batch generation cost is paid only once. Each iteration shares the
/// batches (zero-copy) and creates a fresh reducer + appender.
template<
  int NumBatches,
  int RecordsPerBatch,
  size_t KeySize,
  model::compression Compression>
struct filter_bench {
    static constexpr int total_records = NumBatches * RecordsPerBatch;

    // Built once in constructor, shared (zero-copy) each iteration.
    std::vector<model::record_batch> batch_data = make_bench_batches(
      NumBatches, RecordsPerBatch, KeySize, Compression);

    ss::future<> run_bench(keep_policy policy) {
        auto batches = share_batches(batch_data);
        auto last_offset = batches.back().last_offset();

        tmpbuf_file::store_t store;
        storage::storage_resources resources;
        auto file = ss::file(ss::make_shared<tmpbuf_file>(store));
        storage::segment_appender::options opts(
          std::nullopt, resources, nullptr);
        auto appender = std::make_unique<storage::segment_appender>(
          std::move(file), opts);
        auto stm_mgr = ss::make_lw_shared<storage::stm_manager>();
        noop_compacted_index_writer cidx;

        auto reducer = storage::internal::copy_data_segment_reducer(
          model::ntp(
            model::ns{"test"}, model::topic{"topic"}, model::partition_id{0}),
          make_filter(policy),
          appender.get(),
          /*internal_topic=*/false,
          storage::offset_delta_time{false},
          /*index_base_offset=*/model::offset{0},
          last_offset,
          /*compaction_placeholder_enabled=*/true,
          /*unset_transaction_bit_enabled=*/true,
          stm_mgr,
          &cidx);

        perf_tests::start_measuring_time();
        for (auto& batch : batches) {
            co_await reducer(std::move(batch));
        }
        perf_tests::stop_measuring_time();

        perf_tests::do_not_optimize(reducer.end_of_stream());
        co_await appender->close();
    }
};

// Concrete fixture types — uncompressed.
using bench_b10_r1k_k016 = filter_bench<10, 1000, 16, model::compression::none>;
using bench_b10_r1k_k256
  = filter_bench<10, 1000, 256, model::compression::none>;
using bench_b1k_r10_k016 = filter_bench<1000, 10, 16, model::compression::none>;
using bench_b1k_r10_k256
  = filter_bench<1000, 10, 256, model::compression::none>;

// Concrete fixture types — zstd compressed.
using bench_b10_r1k_k016_zstd
  = filter_bench<10, 1000, 16, model::compression::zstd>;
using bench_b10_r1k_k256_zstd
  = filter_bench<10, 1000, 256, model::compression::zstd>;
using bench_b1k_r10_k016_zstd
  = filter_bench<1000, 10, 16, model::compression::zstd>;
using bench_b1k_r10_k256_zstd
  = filter_bench<1000, 10, 256, model::compression::zstd>;

} // namespace

// clang-format off

// ── 100 batches × 1000 records, 16-byte keys ────────────────────────────

PERF_TEST_C(bench_b10_r1k_k016, keep_none)      { co_await run_bench(keep_policy::none); }
PERF_TEST_C(bench_b10_r1k_k016, keep_all)       { co_await run_bench(keep_policy::all); }
PERF_TEST_C(bench_b10_r1k_k016, keep_half)      { co_await run_bench(keep_policy::half); }

// ── 10 batches × 1000 records, 256-byte keys ───────────────────────────

PERF_TEST_C(bench_b10_r1k_k256, keep_none)      { co_await run_bench(keep_policy::none); }
PERF_TEST_C(bench_b10_r1k_k256, keep_all)       { co_await run_bench(keep_policy::all); }
PERF_TEST_C(bench_b10_r1k_k256, keep_half)      { co_await run_bench(keep_policy::half); }

// ── 1000 batches × 10 records, 16-byte keys ────────────────────────────

PERF_TEST_C(bench_b1k_r10_k016, keep_none)      { co_await run_bench(keep_policy::none); }
PERF_TEST_C(bench_b1k_r10_k016, keep_all)       { co_await run_bench(keep_policy::all); }
PERF_TEST_C(bench_b1k_r10_k016, keep_half)      { co_await run_bench(keep_policy::half); }

// ── 1000 batches × 10 records, 256-byte keys ───────────────────────────

PERF_TEST_C(bench_b1k_r10_k256, keep_none)      { co_await run_bench(keep_policy::none); }
PERF_TEST_C(bench_b1k_r10_k256, keep_all)       { co_await run_bench(keep_policy::all); }
PERF_TEST_C(bench_b1k_r10_k256, keep_half)      { co_await run_bench(keep_policy::half); }

// ── 10 batches × 1000 records, 16-byte keys, zstd ───────────────────────

PERF_TEST_C(bench_b10_r1k_k016_zstd, keep_none) { co_await run_bench(keep_policy::none); }
PERF_TEST_C(bench_b10_r1k_k016_zstd, keep_all)  { co_await run_bench(keep_policy::all); }
PERF_TEST_C(bench_b10_r1k_k016_zstd, keep_half) { co_await run_bench(keep_policy::half); }

// ── 10 batches × 1000 records, 256-byte keys, zstd ──────────────────────

PERF_TEST_C(bench_b10_r1k_k256_zstd, keep_none) { co_await run_bench(keep_policy::none); }
PERF_TEST_C(bench_b10_r1k_k256_zstd, keep_all)  { co_await run_bench(keep_policy::all); }
PERF_TEST_C(bench_b10_r1k_k256_zstd, keep_half) { co_await run_bench(keep_policy::half); }

// ── 1000 batches × 10 records, 16-byte keys, zstd ───────────────────────

PERF_TEST_C(bench_b1k_r10_k016_zstd, keep_none) { co_await run_bench(keep_policy::none); }
PERF_TEST_C(bench_b1k_r10_k016_zstd, keep_all)  { co_await run_bench(keep_policy::all); }
PERF_TEST_C(bench_b1k_r10_k016_zstd, keep_half) { co_await run_bench(keep_policy::half); }

// ── 1000 batches × 10 records, 256-byte keys, zstd ──────────────────────

PERF_TEST_C(bench_b1k_r10_k256_zstd, keep_none) { co_await run_bench(keep_policy::none); }
PERF_TEST_C(bench_b1k_r10_k256_zstd, keep_all)  { co_await run_bench(keep_policy::all); }
PERF_TEST_C(bench_b1k_r10_k256_zstd, keep_half) { co_await run_bench(keep_policy::half); }

// clang-format on
