// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "model/record.h"
#include "storage/key_offset_map.h"

#include <seastar/core/loop.hh>

#include <memory>
#include <ostream>
#include <utility>

// An implementation for the sliding window algorithm for compaction.
// Sliding window algorithm is performed in two steps:
// 1. A backward pass over the data source, in which the latest key-offset pair
// is indexed in a hash map of a finite size (determined by cluster config
// `storage_compaction_key_map_memory`) until all keys from the data source are
// indexed OR the hash map's size has reached its allocated capacity.
// 2. A forward pass over the data source, in which the data (i.e Kafka
// records) is filtered and rewritten.
// Filtering can be a removal of data due to:
// 1. De-duplication (the removal of data due to a newer key appearing in the
// data source).
// 2. Deletion (the removal of data due to other logic, i.e the removal of a
// tombstone record which has passed the time horizon set by
// `delete.retention.ms`)
// An example usage of this class is the following:
// auto src  = std::make_unique<segment_source>(_segs);
// auto sink = std::make_unique<segment_sink>();
// auto reducer = compaction_reducer(std::move(src), std::move(sink));
// auto res = co_await std::move(reducer).run();
class compaction_reducer {
public:
    // The sink for the data to be written by this round of compaction.
    // This class needs to implement just one function for the sliding window
    // algorithm:
    // 1. `operator()(record)`: This operator accepts a record (which
    // has already been determined to be written by the
    // `compaction_reducer_source` below) and is responsible for writing its
    // contents to whichever data format/store this `compaction_reducer_sink`
    // represents.
    class compaction_reducer_sink {
    public:
        compaction_reducer_sink() noexcept = default;
        compaction_reducer_sink(compaction_reducer_sink&& o) noexcept = default;
        compaction_reducer_sink& operator=(compaction_reducer_sink&& o) noexcept
          = default;
        compaction_reducer_sink(const compaction_reducer_sink& o) = delete;
        compaction_reducer_sink& operator=(const compaction_reducer_sink& o)
          = delete;
        virtual ~compaction_reducer_sink() noexcept = default;

    public:
        virtual ss::future<> operator()(const model::record& r) = 0;
    };

    // The source of data for compaction.
    // This class needs to implement two key functions for the sliding window
    // algorithm:
    // 1. `backward_pass_iteration(map)`: This is the pass that reads from the
    // data source from head to tail, and indexes the latest key-offset pair in
    // the provided map. This should ideally be a light-weight read over a
    // portion of the log that avoids de-compression or e.g. uncached reads from
    // cloud storage.
    // 2. `forward_pass_iteration(sink, map)`: This is the pass that reads from
    // the data source from tail to head, and provides the data to be written
    // (determined by the contents of the key-offset map) for this round of
    // compaction to the sink object.
    class compaction_reducer_source {
    public:
        compaction_reducer_source() noexcept = default;
        compaction_reducer_source(compaction_reducer_source&& o) noexcept
          = default;
        compaction_reducer_source&
        operator=(compaction_reducer_source&& o) noexcept
          = default;
        compaction_reducer_source(const compaction_reducer_source& o) = delete;
        compaction_reducer_source& operator=(const compaction_reducer_source& o)
          = delete;
        virtual ~compaction_reducer_source() noexcept = default;

    public:
        virtual ss::future<ss::stop_iteration>
        backward_pass_iteration(storage::key_offset_map& map) const = 0;
        virtual ss::future<ss::stop_iteration> forward_pass_iteration(
          compaction_reducer_sink&, storage::key_offset_map& map) const
          = 0;

    private:
    };

    struct stats {
        // Total number of batches passed to this reducer.
        size_t batches_processed{0};
        // Number of batches that were completely removed.
        size_t batches_discarded{0};
        // Number of records removed by this reducer, including batches that
        // were entirely removed.
        size_t records_discarded{0};
        // Number of batches that were ignored because they are not
        // of a compactible type.
        size_t non_compactible_batches{0};

        // Returns whether any data was removed by this reducer.
        bool has_removed_data() const {
            return batches_discarded > 0 || records_discarded > 0;
        }

        friend std::ostream& operator<<(std::ostream& os, const stats& s) {
            fmt::print(
              os,
              "{{ batches_processed: {}, batches_discarded: {}, "
              "records_discarded: {}, non_compactible_batches: {} }}",
              s.batches_processed,
              s.batches_discarded,
              s.records_discarded,
              s.non_compactible_batches);
            return os;
        }
    };

public:
    explicit compaction_reducer(
      std::unique_ptr<compaction_reducer_source> src,
      std::unique_ptr<compaction_reducer_sink> sink) noexcept
      : _src(std::move(src))
      , _sink(std::move(sink)) {}
    compaction_reducer(const compaction_reducer&) = delete;
    compaction_reducer& operator=(const compaction_reducer&) = delete;
    compaction_reducer(compaction_reducer&&) noexcept = default;
    compaction_reducer& operator=(compaction_reducer&&) noexcept = default;
    ~compaction_reducer() noexcept = default;

    ss::future<stats> run() &&;

private:
    stats end_of_stream();
    compaction_reducer() = default;

    storage::key_offset_map& _map;
    std::unique_ptr<compaction_reducer_source> _src;
    std::unique_ptr<compaction_reducer_sink> _sink;
};
