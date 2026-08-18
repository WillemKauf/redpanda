// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "model/record.h"
#include "model/record_utils.h"
#include "model/tests/random_batch.h"
#include "storage/parser.h"
#include "storage/parser_errc.h"
#include "storage/record_batch_utils.h"
#include "storage/segment_reader.h"

#include <gtest/gtest.h>

#include <vector>

namespace {

using storage::record_version_type;

iobuf serialize_header(
  const model::record_batch_header& h, record_version_type fmt) {
    return fmt == record_version_type::v1
             ? storage::batch_header_to_disk_iobuf(h)
             : storage::v2_batch_header_to_disk_iobuf(h);
}

iobuf serialize_stream(
  const chunked_circular_buffer<model::record_batch>& batches,
  record_version_type fmt) {
    iobuf out;
    for (const auto& b : batches) {
        out.append(serialize_header(b.header(), fmt));
        out.append(b.data().copy());
    }
    return out;
}

// records everything the parser reports, accepting or skipping all batches
struct collecting_consumer final : storage::batch_consumer {
    explicit collecting_consumer(consume_result decision)
      : decision(decision) {}

    consume_result
    accept_batch_start(const model::record_batch_header&) const final {
        return decision;
    }

    void consume_batch_start(
      model::record_batch_header h,
      size_t physical_base_offset,
      size_t size_on_disk) final {
        headers.push_back(h);
        physical_offsets.push_back(physical_base_offset);
        disk_sizes.push_back(size_on_disk);
    }

    void skip_batch_start(
      model::record_batch_header h,
      size_t physical_base_offset,
      size_t size_on_disk) final {
        headers.push_back(h);
        physical_offsets.push_back(physical_base_offset);
        disk_sizes.push_back(size_on_disk);
    }

    void consume_records(iobuf&& b) final { records.push_back(std::move(b)); }

    ss::future<stop_parser> consume_batch_end() final {
        return ss::make_ready_future<stop_parser>(stop_parser::no);
    }

    fmt::iterator format_to(fmt::iterator it) const final {
        return fmt::format_to(it, "collecting_consumer");
    }

    consume_result decision;
    std::vector<model::record_batch_header> headers;
    std::vector<size_t> physical_offsets;
    std::vector<size_t> disk_sizes;
    std::vector<iobuf> records;
};

struct parse_result {
    result<size_t> consumed{0};
    storage::parser_errc errc{storage::parser_errc::none};
    std::unique_ptr<collecting_consumer> consumer;
};

parse_result parse(
  iobuf buf,
  record_version_type fmt,
  storage::batch_consumer::consume_result decision
  = storage::batch_consumer::consume_result::accept_batch,
  bool recovery = false) {
    auto consumer = std::make_unique<collecting_consumer>(decision);
    auto* c = consumer.get();
    auto parser = storage::continuous_batch_parser(
      std::move(consumer),
      storage::segment_reader_handle(make_iobuf_input_stream(std::move(buf))),
      fmt,
      recovery);
    parse_result r;
    r.consumed = parser.consume().get();
    r.errc = parser.error();
    parser.close().get();
    // steal the consumer state back out of the parser for inspection
    r.consumer = std::make_unique<collecting_consumer>(std::move(*c));
    return r;
}

chunked_circular_buffer<model::record_batch>
make_batches(model::term_id term = model::term_id(3)) {
    auto batches = model::test::make_random_batches(model::offset(0)).get();
    for (auto& b : batches) {
        b.set_term(term);
    }
    return batches;
}

} // namespace

// Both header formats must deliver the batches' headers verbatim and their
// records to the consumer, with physical offsets and on-disk sizes
// reflecting the actual bytes each format occupies on disk.
TEST(ParserHeaderFormat, parses_both_formats) {
    for (auto fmt : {record_version_type::v1, record_version_type::v2}) {
        auto batches = make_batches();
        auto buf = serialize_stream(batches, fmt);
        const auto total_size = buf.size_bytes();

        auto r = parse(std::move(buf), fmt);
        ASSERT_TRUE(r.consumed.has_value());
        EXPECT_EQ(r.consumed.value(), total_size);

        const auto& c = *r.consumer;
        ASSERT_EQ(c.headers.size(), batches.size());
        size_t expected_offset = 0;
        auto it = batches.begin();
        for (size_t i = 0; i < c.headers.size(); ++i, ++it) {
            const auto& expected = it->header();
            const auto& got = c.headers[i];
            EXPECT_EQ(got, expected);
            EXPECT_EQ(got.header_crc, expected.header_crc);
            if (fmt == record_version_type::v2) {
                // the v2 format persists the term, v1 cannot
                EXPECT_EQ(got.ctx.term, expected.ctx.term);
            }
            EXPECT_EQ(c.records[i], it->data());

            const auto batch_disk_size = storage::batch_on_disk_size(
              expected, fmt);
            EXPECT_EQ(c.disk_sizes[i], batch_disk_size);
            EXPECT_EQ(c.physical_offsets[i], expected_offset);
            expected_offset += batch_disk_size;
        }
        EXPECT_EQ(expected_offset, total_size);
    }
}

// Skipped batches must advance physical offsets and consumed bytes exactly
// like accepted ones.
TEST(ParserHeaderFormat, skipping_advances_physical_offsets) {
    for (auto fmt : {record_version_type::v1, record_version_type::v2}) {
        auto batches = make_batches();
        auto buf = serialize_stream(batches, fmt);
        const auto total_size = buf.size_bytes();

        auto accepted = parse(buf.copy(), fmt);
        auto skipped = parse(
          std::move(buf),
          fmt,
          storage::batch_consumer::consume_result::skip_batch);

        ASSERT_TRUE(skipped.consumed.has_value());
        EXPECT_EQ(skipped.consumed.value(), total_size);
        EXPECT_TRUE(skipped.consumer->records.empty());
        EXPECT_EQ(
          skipped.consumer->physical_offsets,
          accepted.consumer->physical_offsets);
        EXPECT_EQ(skipped.consumer->disk_sizes, accepted.consumer->disk_sizes);
    }
}

// A corrupted header must stop the parser with a CRC mismatch in both
// formats.
TEST(ParserHeaderFormat, detects_header_corruption) {
    for (auto fmt : {record_version_type::v1, record_version_type::v2}) {
        auto batches = make_batches();
        auto buf = serialize_stream(batches, fmt);

        // flip a byte within the first header's checksummed region: the
        // producer_id field lives past the first 32 bytes of the header in
        // both formats
        auto corrupted = iobuf();
        auto in = iobuf::iterator_consumer(buf.cbegin(), buf.cend());
        std::vector<char> bytes(buf.size_bytes());
        in.consume_to(bytes.size(), bytes.data());
        bytes[40] = static_cast<char>(~bytes[40]);
        corrupted.append(bytes.data(), bytes.size());

        auto r = parse(std::move(corrupted), fmt);
        EXPECT_EQ(r.errc, storage::parser_errc::header_only_crc_missmatch);
        EXPECT_TRUE(r.consumer->headers.empty());
    }
}

// A stream that ends mid-header must surface a short-read error, not a
// parse failure.
TEST(ParserHeaderFormat, truncated_header) {
    for (auto fmt : {record_version_type::v1, record_version_type::v2}) {
        auto batches = make_batches();
        auto buf = serialize_stream(batches, fmt);
        auto truncated = buf.share(0, 20);

        auto r = parse(std::move(truncated), fmt, {}, /*recovery=*/true);
        EXPECT_EQ(r.errc, storage::parser_errc::input_stream_not_enough_bytes);
    }
}

// An all-zero tail (a fallocated file range) is a benign end-of-data
// condition: everything before it must be consumed.
TEST(ParserHeaderFormat, fallocated_zero_tail) {
    for (auto fmt : {record_version_type::v1, record_version_type::v2}) {
        auto batches = make_batches();
        auto buf = serialize_stream(batches, fmt);
        const auto data_size = buf.size_bytes();
        const std::vector<char> zeros(4096, 0);
        buf.append(zeros.data(), zeros.size());

        auto r = parse(std::move(buf), fmt);
        ASSERT_TRUE(r.consumed.has_value());
        EXPECT_EQ(r.consumed.value(), data_size);
        EXPECT_EQ(
          r.errc,
          storage::parser_errc::fallocated_file_read_zero_bytes_for_header);
        EXPECT_EQ(r.consumer->headers.size(), batches.size());
    }
}

// transform_stream must be able to convert between header formats in both
// directions, byte-for-byte, while the predicate stamps the term -- the
// mechanism self compaction will use to rewrite v1 segments as v2.
TEST(ParserHeaderFormat, transform_stream_converts_formats) {
    const auto term = model::term_id(7);
    auto batches = make_batches(term);
    auto v1_bytes = serialize_stream(batches, record_version_type::v1);
    auto v2_bytes = serialize_stream(batches, record_version_type::v2);

    auto run_transform = [&](
                           iobuf in,
                           record_version_type in_fmt,
                           record_version_type out_fmt) {
        iobuf out;
        auto res
          = storage::transform_stream(
              make_iobuf_input_stream(std::move(in)),
              make_iobuf_ref_output_stream(out),
              [term](model::record_batch_header& h) {
                  h.ctx.term = term;
                  return storage::batch_consumer::consume_result::accept_batch;
              },
              in_fmt,
              out_fmt)
              .get();
        EXPECT_TRUE(res.has_value());
        EXPECT_EQ(res.value(), out.size_bytes());
        return out;
    };

    auto converted_v2 = run_transform(
      v1_bytes.copy(), record_version_type::v1, record_version_type::v2);
    EXPECT_EQ(converted_v2, v2_bytes);

    auto converted_v1 = run_transform(
      v2_bytes.copy(), record_version_type::v2, record_version_type::v1);
    EXPECT_EQ(converted_v1, v1_bytes);
}
