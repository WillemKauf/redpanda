// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/fundamental.h"
#include "model/record_utils.h"
#include "model/tests/random_batch.h"
#include "storage/api.h"
#include "storage/directories.h"
#include "storage/log_replayer.h"
#include "storage/parser.h"
#include "storage/segment.h"
#include "storage/segment_appender.h"
#include "storage/segment_reader.h"
#include "storage/version.h"
#include "test_utils/random_bytes.h"
#include "utils/null_output_stream.h"

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

using namespace std::chrono_literals; // NOLINT
using namespace storage;              // NOLINT

void write_garbage(segment_appender& ptr) {
    auto b = tests::random_bytes(100);
    // NOLINTNEXTLINE
    ptr.append(reinterpret_cast<const char*>(b.data()), b.size()).get();
    ptr.flush().get();
}

void write_batches(ss::lw_shared_ptr<segment> seg) {
    auto batches = model::test::make_random_batches(
                     seg->offsets().get_base_offset() + model::offset(1), 1)
                     .get();
    for (auto& b : batches) {
        b.header().header_crc = model::internal_header_only_crc(b.header());
        (void)seg->append(std::move(b)).get();
    }
    seg->flush().get();
}

inline ss::sstring test_directory() {
    char* tmpdir = std::getenv("TEST_TMPDIR");
    if (!tmpdir) {
        return "test.dir";
    }
    return {std::filesystem::path(tmpdir) / std::string("test.dir")};
}

log_config make_config() {
    return log_config{
      test_directory(), 1024, storage::make_sanitized_file_config()};
}

ntp_config config_from_ntp(const model::ntp& ntp) {
    return ntp_config(ntp, test_directory());
}

constexpr size_t default_segment_readahead_size = 128 * 1024;
constexpr unsigned default_segment_readahead_count = 10;

// A v2 segment created through the log manager must carry the v2 filename,
// serialize appended batch headers via serde (persisting the term), and be
// readable and recoverable end to end.
TEST(LogManagerTest, test_v2_segment_round_trip) {
    auto conf = make_config();

    ss::sharded<features::feature_table> feature_table;
    feature_table.start().get();
    feature_table
      .invoke_on_all(
        [](features::feature_table& f) { f.testing_activate_all(); })
      .get();

    storage::api store(
      [conf]() {
          return storage::kvstore_config(
            1_MiB,
            config::mock_binding(10ms),
            conf.base_dir,
            storage::make_sanitized_file_config());
      },
      [conf]() { return conf; },
      feature_table);
    store.start().get();
    auto stop_kvstore = ss::defer([&store, &feature_table] {
        store.stop().get();
        feature_table.stop().get();
    });

    const auto term = model::term_id(3);
    auto ntp_cfg = config_from_ntp(model::ntp("kafka", "topic-v2", 0));
    directories::initialize(ntp_cfg.work_directory()).get();
    auto seg = store.log_mgr()
                 .make_log_segment(
                   ntp_cfg,
                   model::offset(0),
                   term,
                   default_segment_readahead_size,
                   default_segment_readahead_count,
                   1_MiB,
                   record_version_type::v2)
                 .get();
    auto close_seg = ss::defer([&seg] { seg->close().get(); });
    ASSERT_EQ(seg->reader().path().get_version(), record_version_type::v2);
    ASSERT_TRUE(seg->reader().filename().ends_with("-3-v2.log"));

    auto batches = model::test::make_random_batches(model::offset(0), 10).get();
    for (auto& b : batches) {
        b.set_term(term);
        b.header().header_crc = model::internal_header_only_crc(b.header());
        (void)seg->append(b.share()).get();
    }
    seg->flush().get();
    seg->reader().set_file_size(seg->appender().file_byte_offset());

    // scan the segment the way the read path does, collecting the headers
    std::vector<model::record_batch_header> headers;
    auto handle = seg->reader().data_stream(0).get();
    auto res = transform_stream(
                 handle.take_stream(),
                 utils::make_null_output_stream(),
                 [&headers](model::record_batch_header& h) {
                     headers.push_back(h);
                     return batch_consumer::consume_result::accept_batch;
                 },
                 record_version_type::v2,
                 record_version_type::v2)
                 .get();
    handle.close().get();
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res.value(), seg->appender().file_byte_offset());
    ASSERT_EQ(headers.size(), batches.size());
    auto it = batches.begin();
    for (const auto& h : headers) {
        EXPECT_EQ(h, it->header());
        EXPECT_EQ(h.ctx.term, term);
        ++it;
    }

    // and recover it the way startup does
    auto recovered = log_replayer(*seg).recover_in_thread();
    ASSERT_TRUE(bool(recovered));
    EXPECT_EQ(recovered.last_offset.value(), batches.back().last_offset());
}

TEST(LogManagerTest, test_can_load_logs) {
    auto conf = make_config();

    ss::logger test_logger("test-logger");
    ss::sharded<features::feature_table> feature_table;
    feature_table.start().get();
    feature_table
      .invoke_on_all(
        [](features::feature_table& f) { f.testing_activate_all(); })
      .get();

    storage::api store(
      [conf]() {
          return storage::kvstore_config(
            1_MiB,
            config::mock_binding(10ms),
            conf.base_dir,
            storage::make_sanitized_file_config());
      },
      [conf]() { return conf; },
      feature_table);
    store.start().get();
    auto stop_kvstore = ss::defer([&store, &feature_table] {
        store.stop().get();
        feature_table.stop().get();
    });
    auto& m = store.log_mgr();
    std::vector<storage::ntp_config> ntps;
    ntps.reserve(4);
    for (size_t i = 0; i < 4; ++i) {
        ntps.push_back(
          config_from_ntp(model::ntp(ssx::sformat("ns{}", i), "topic-1", i)));
        directories::initialize(ntps[i].work_directory()).get();
    }
    auto seg = m.make_log_segment(
                  ntps[0],
                  model::offset(10),
                  model::term_id(1),
                  default_segment_readahead_size,
                  default_segment_readahead_count,
                  0)
                 .get();
    seg->close().get();

    // auto ntp2 = empty

    auto seg3 = m.make_log_segment(
                   ntps[2],
                   model::offset(20),
                   model::term_id(1),
                   default_segment_readahead_size,
                   default_segment_readahead_count,
                   1_MiB)
                  .get();
    write_batches(seg3);
    seg3->close().get();

    auto seg4 = m.make_log_segment(
                   ntps[3],
                   model::offset(2),
                   model::term_id(1),
                   default_segment_readahead_size,
                   default_segment_readahead_count,
                   1_MiB)
                  .get();
    write_garbage(seg4->appender());
    seg4->close().get();

    std::vector<ss::shared_ptr<storage::log>> logs;
    for (size_t i = 0; i < 4; ++i) {
        auto log = m.manage(config_from_ntp(ntps[i].ntp())).get();
        log->stm_hookset()->start();
        logs.push_back(std::move(log));
    }
    auto stop_stms = ss::defer([&logs] {
        for (auto& log : logs) {
            log->stm_hookset()->stop();
        }
    });
    EXPECT_EQ(4, m.size());
    EXPECT_EQ(m.get(ntps[0].ntp())->segment_count(), 0);
    EXPECT_EQ(m.get(ntps[1].ntp())->segment_count(), 0);
    EXPECT_EQ(m.get(ntps[2].ntp())->segment_count(), 1);
    EXPECT_EQ(m.get(ntps[3].ntp())->segment_count(), 0);
    EXPECT_FALSE(file_exists(seg->reader().filename()).get());
    EXPECT_TRUE(file_exists(seg3->reader().filename()).get());
    EXPECT_FALSE(file_exists(seg4->reader().filename()).get());
    EXPECT_TRUE(
      file_exists(seg4->reader().filename() + ".cannotrecover").get());
}
