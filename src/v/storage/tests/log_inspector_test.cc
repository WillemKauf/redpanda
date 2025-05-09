
#include "config/property.h"
#include "kafka/server/group_data_parser.h"
#include "model/fundamental.h"
#include "raft/types.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "storage/api.h"
#include "storage/kvstore.h"
#include "storage/log_manager.h"
#include "storage/offset_translator_state.h"
#include "test_utils/test.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/file.hh>

#include <cstdint>
#include <optional>
#include <string_view>

using namespace std::chrono_literals;

std::string_view dir = "/home/willem/Documents/temp/co/redpanda/data";

storage::kvstore_config kv_cfg() {
    return {10_MiB, config::mock_binding(10ms), ss::sstring(dir), std::nullopt};
}

storage::log_config log_mgr_cfg() { return {ss::sstring(dir), 1_MiB}; }

struct printing_consumer {
    ss::future<ss::stop_iteration>
    operator()(const model::record_batch& batch) {
        fmt::print("batch: {}\n", batch.header());
        co_return ss::stop_iteration::no;
    }

    void end_of_stream() {}
};
static auto serializer = kafka::make_consumer_offsets_serializer();
void handle_record(model::record r) {
    try {
        auto record_type = serializer.get_metadata_type(r.key().copy());
        switch (record_type) {
        case kafka::offset_commit: {
            auto v = serializer.decode_offset_metadata(std::move(r));
            fmt::print(
              "offset_commit: {}/{} offset: {}\n",
              v.key.topic,
              v.key.partition,
              v.value->offset);
            return;
        }
        case kafka::group_metadata:
            serializer.decode_group_metadata(std::move(r));
            return;
        case kafka::noop:
            // ignore noops, they are handled for backward compatibility
            return;
        }
        __builtin_unreachable();
    } catch (...) {
    }
}

struct co_printing_consumer {
    ss::future<ss::stop_iteration>
    operator()(const model::record_batch& batch) {
        fmt::print("batch: {}\n", batch.header());
        batch.for_each_record(
          [&](model::record r) { handle_record(std::move(r)); });
        co_return ss::stop_iteration::no;
    }

    void end_of_stream() {}
};

struct collecting_consumer {
    chunked_vector<model::record_batch_header> headers;
    ss::future<ss::stop_iteration>
    operator()(const model::record_batch& batch) {
        if (
          batch.header().type == model::record_batch_type::archival_metadata) {
            headers.push_back(batch.header());
        }
        co_return ss::stop_iteration::no;
    }

    chunked_vector<model::record_batch_header> end_of_stream() {
        return std::move(headers);
    }
};
using log_ptr = ss::shared_ptr<storage::log>;
ss::future<fragmented_vector<model::record_batch>>
read_log(log_ptr log, model::offset next_to_read) {
    storage::log_reader_config config(
      next_to_read,
      model::offset::max(),
      0,
      1024 * 1024,
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt,
      std::nullopt);
    config.fill_gaps = true;
    auto reader = co_await log->make_reader(config);

    co_return co_await model::consume_reader_to_fragmented_memory(
      std::move(reader), model::no_timeout);
}

using log_ptr = ss::shared_ptr<storage::log>;
ss::future<fragmented_vector<model::record_batch>> read_kafka_log(log_ptr log) {
    storage::log_reader_config config(
      log->offsets().start_offset,
      model::model_limits<model::offset>::max(),
      0,
      std::numeric_limits<size_t>::max(),
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt,
      std::nullopt);

    config.fill_gaps = false;
    auto reader = co_await log->make_reader(config);

    co_return co_await model::consume_reader_to_fragmented_memory(
      std::move(reader), model::no_timeout);
}

ss::future<> print_kafka_log(log_ptr log) {
    storage::log_reader_config config(
      log->offsets().start_offset,
      model::model_limits<model::offset>::max(),
      0,
      std::numeric_limits<size_t>::max(),
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt,
      std::nullopt);

    config.fill_gaps = false;
    auto reader = co_await log->make_reader(config);
    co_await reader.for_each_ref(co_printing_consumer{}, model::no_timeout);
}

ss::future<> read_in_ranges(log_ptr log) {
    model::offset next_to_read = log->offsets().start_offset;

    while (next_to_read < log->offsets().committed_offset) {
        auto batches = co_await read_log(log, next_to_read);
        next_to_read = model::next_offset(batches.back().last_offset());
        for (auto& batch : batches) {
            if (
              batch.header().type != model::record_batch_type::raft_data
              && batch.header().type != model::record_batch_type::ghost_batch) {
                fmt::print("batch: {}\n", batch.header());
            }
        }
    }

    co_return;
}

ss::future<chunked_vector<model::record_batch_header>>
read_archival_batches(log_ptr log) {
    chunked_vector<model::record_batch_header> headers;
    model::offset next_to_read = log->offsets().start_offset;

    while (next_to_read < log->offsets().committed_offset) {
        auto batches = co_await read_log(log, next_to_read);
        next_to_read = model::next_offset(batches.back().last_offset());
        for (auto& batch : batches) {
            if (
              batch.header().type
              == model::record_batch_type::archival_metadata) {
                // fmt::print("batch: {}\n", batch.header());
                headers.push_back(batch.header());
            }
        }
    }
    co_return headers;
}

ss::future<chunked_vector<model::record_batch_header>>
read_archival_batches_reader(log_ptr log) {
    storage::log_reader_config config(
      log->offsets().start_offset,
      model::offset::max(),
      0,
      std::numeric_limits<size_t>::max(),
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt,
      std::nullopt);

    auto reader = co_await log->make_reader(config);
    co_return co_await reader.for_each_ref(
      collecting_consumer{}, model::no_timeout);
}

ss::future<> copy_in_ranges(log_ptr source, log_ptr target) {
    model::offset next_to_read = source->offsets().start_offset;

    while (next_to_read < source->offsets().committed_offset) {
        auto batches = co_await read_log(source, next_to_read);
        auto last_read = batches.back().last_offset();
        next_to_read = model::next_offset(batches.back().last_offset());
        auto appender = target->make_appender(storage::log_append_config{
          .should_fsync = storage::log_append_config::fsync::no,
          .io_priority = ss::default_priority_class(),
          .timeout = model::no_timeout});
        auto ar = co_await model::make_fragmented_memory_record_batch_reader(
                    std::move(batches))
                    .for_each_ref(std::move(appender), model::no_timeout);
        vassert(ar.last_offset == last_read, "last offset mismatch");
    }
    co_return;
}

model::record_batch make_archival_batch() {
    storage::record_batch_builder builder(
      model::record_batch_type::archival_metadata, model::offset(0));
    for (int i = 0; i < random_generators::get_int(1, 10); ++i) {
        builder.add_raw_kv(
          iobuf::from("archival-key"), iobuf::from("archival-value"));
    }
    return std::move(builder).build();
}

model::record_batch make_raft_cfg() {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_configuration, model::offset(0));

    builder.add_raw_kv(iobuf::from("r-key"), iobuf::from("r-value"));

    return std::move(builder).build();
}

iobuf key_buffer() {
    return iobuf::from(fmt::format("key-{}", random_generators::get_int(0, 3)));
}

model::record_batch make_data_batch() {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));

    for (auto i : boost::irange(random_generators::get_int(100, 500))) {
        builder.add_raw_kv(
          key_buffer(), iobuf::from(fmt::format("data-value-{}", i)));
    }

    return std::move(builder).build();
}

static const float archival_frequency = 0.01;
struct producer {
    std::tuple<model::record_batch_reader, bool> generate_batches() {
        chunked_vector<model::record_batch> batches;
        bool archival = false;
        if (
          random_generators::get_int(0, 100) <= archival_frequency * 100
          && segments % 4 == 0) {
            batches.push_back(make_archival_batch());
            archival = true;
        } else if (random_generators::get_int(0, 1000) == 0) {
            batches.push_back(make_raft_cfg());
        } else {
            batches.push_back(make_data_batch());
        }

        return std::make_tuple(
          model::make_fragmented_memory_record_batch_reader(std::move(batches)),
          archival);
    }

    ss::future<> produce_data(log_ptr log, size_t) {
        while (archival_batch_offsets.size() < 1000) {
            auto appender = log->make_appender(storage::log_append_config{
              .should_fsync = storage::log_append_config::fsync::no,
              .io_priority = ss::default_priority_class(),
              .timeout = model::no_timeout});

            auto [reader, archival_batch] = generate_batches();
            auto ar = co_await reader.for_each_ref(
              std::move(appender), model::no_timeout);
            if (archival_batch) {
                archival_batch_offsets.push_back(ar.base_offset);
            }
            segments = log->segment_count();
        }
        co_await log->flush();
    }
    size_t segments = 0;
    chunked_vector<model::offset> archival_batch_offsets;
};
storage::ntp_config make_ntp_config(
  const model::ntp& ntp, model::revision_id rev = model::revision_id(0)) {
    storage::ntp_config test_ntp_cfg(ntp, ss::sstring(dir), nullptr, rev);
    storage::ntp_config::default_overrides o;
    o.cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction;

    o.cache_enabled = storage::with_cache::no;
    o.segment_size = 10_MiB;

    test_ntp_cfg.set_overrides(o);

    return test_ntp_cfg;
}

// TEST(compacted_reads, read_compacted_topic) {
//     ss::sharded<features::feature_table> ft;
//     config::shard_local_cfg().log_compaction_use_sliding_window.set_value(true);
//     ft.start().get();
//     ft.local().testing_activate_all();
//     storage::api storage(&kv_cfg, &log_mgr_cfg, ft);
//     storage.start().get();

//     model::partition_id partition{14};

//     model::ntp compacted_ntp(
//       model::kafka_namespace, model::kafka_consumer_offsets_topic,
//       partition);

//     auto log = storage.log_mgr()
//                  .manage(
//                    make_ntp_config(compacted_ntp,
//                    model::revision_id(23910067)), raft::group_id(0),
//                    model::offset_translator_batch_types())
//                  .get();
//     log->start(std::nullopt).get();
//     kafka::group_recovery_consumer consumer(
//       log, model::no_timeout, ss::default_priority_class());

//     storage.stop().get();
// }

TEST(compacted_reads, read_compacted_topic) {
    ss::sharded<features::feature_table> ft;
    config::shard_local_cfg().log_compaction_use_sliding_window.set_value(true);
    ft.start().get();
    ft.local().testing_activate_all();
    storage::api storage(&kv_cfg, &log_mgr_cfg, ft);
    storage.start().get();

    model::partition_id partition{13};

    model::ntp compacted_ntp(
      model::kafka_namespace, model::kafka_consumer_offsets_topic, partition);

    auto log = storage.log_mgr()
                 .manage(
                   make_ntp_config(compacted_ntp, model::revision_id(23917634)),
                   raft::group_id(0),
                   model::offset_translator_batch_types())
                 .get();
    log->start(std::nullopt).get();
    print_kafka_log(log).get();

    ss::abort_source as;
    for (auto i = 0; i < 50; ++i) {
        log
          ->housekeeping(storage::housekeeping_config(
            model::timestamp::max(),
            std::nullopt,
            model::offset::max(),
            std::nullopt,
            ss::default_priority_class(),
            as,
            std::nullopt))
          .get();
    }
    fmt::print(">>> COMPACTED LOGS\n");
    print_kafka_log(log).get();

    storage.stop().get();
    // ft.stop().get();
    ASSERT_TRUE(false);
}
