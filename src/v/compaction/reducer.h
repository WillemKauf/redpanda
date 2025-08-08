// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "compaction/types.h"
#include "model/compression.h"
#include "model/record.h"

#include <seastar/core/loop.hh>

#include <memory>
#include <ostream>
#include <utility>

namespace compaction {

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
// auto src  = std::make_unique<storage::segment_source>(_segs);
// auto sink = std::make_unique<storage::segment_sink>();
// auto reducer = compaction::reducer(std::move(src), std::move(sink));
// auto res = co_await std::move(reducer).run();
class reducer {
public:
    // The sink for the data to be written by this round of compaction.
    // This class needs to implement two functions:
    // 1. `initialize(cfg)`: This performs initial set-up (if required) per the
    // provided `compaction_config` object. For example, the local storage
    // source may need to collect the set of `segment`s eligible for compaction,
    // and perform self compaction on them before proceeding to the sliding
    // window algorithm.
    // 2. `operator()(record)`: This operator accepts a record (which
    // has already been determined to be written by the `source` below) and is
    // responsible for writing its contents to whichever data format/store this
    // `sink` represents.
    // 3. `finalize()`: perform any final steps required in the `sink` layer,
    // i.e flushing in progress writes, update final metadata, etc.
    class sink {
    public:
        sink() noexcept = default;
        sink(sink&& o) noexcept = default;
        sink& operator=(sink&& o) noexcept = default;
        sink(const sink& o) = delete;
        sink& operator=(const sink& o) = delete;
        virtual ~sink() noexcept = default;

    public:
        virtual ss::future<ss::stop_iteration>
        operator()(model::record_batch b, model::compression c) = 0;
        virtual ss::future<> finalize() = 0;
    };

    // The source of data for compaction.
    // This class needs to implement three functions:
    // 1. `initialize(cfg)`: This performs initial set-up (if required) per the
    // provided `compaction_config` object. For example, the local storage
    // source may need to collect the set of `segment`s eligible for compaction,
    // and perform self compaction on them before proceeding to the sliding
    // window algorithm.
    // 2. `is_end_of_stream()`: This returns a `bool` indicating whether the
    // source has data left or not.
    // 3. `end_of_stream()`: This returns a `bool` indicating whether the
    // forward pass should be taken or not after completing the backward pass.
    // 4. `backward_pass_iteration(cfg)`: This is the pass that reads from the
    // data source from head to tail, and indexes the latest key-offset pair in
    // the contained map within the provided `compaction_config` object.This
    // should ideally be a light-weight read over a portion of the log that
    // avoids de-compression or e.g. uncached reads from cloud storage.
    // 5. `forward_pass_iteration(sink, cfg)`: This is the pass that reads from
    // the data source from tail to head, and provides the data to be written
    // (determined by the contents of the key-offset map and other configured
    // parameters within `cfg`) for this round of compaction to the sink object.
    class source {
    public:
        source() noexcept = default;
        source(source&& o) noexcept = default;
        source& operator=(source&& o) noexcept = default;
        source(const source& o) = delete;
        source& operator=(const source& o) = delete;
        virtual ~source() noexcept = default;

    public:
        virtual ss::future<> initialize_source() = 0;
        virtual ss::future<> initialize_sink(sink&) = 0;
        virtual bool is_end_of_stream() const = 0;
        virtual ss::future<bool> end_of_stream() const = 0;
        virtual ss::future<ss::stop_iteration> backward_pass_iteration() = 0;
        virtual ss::future<ss::stop_iteration>
        forward_pass_iteration(sink&) = 0;

    private:
    };

public:
    explicit reducer(
      compaction_config cfg,
      std::unique_ptr<source> src,
      std::unique_ptr<sink> sink) noexcept
      : _cfg(std::move(cfg))
      , _src(std::move(src))
      , _sink(std::move(sink)) {}
    reducer(const reducer&) = delete;
    reducer& operator=(const reducer&) = delete;
    reducer(reducer&&) noexcept = default;
    reducer& operator=(reducer&&) noexcept = default;
    ~reducer() noexcept = default;

    ss::future<> run() &&;

private:
    compaction_config _cfg;
    std::unique_ptr<source> _src;
    std::unique_ptr<sink> _sink;
};

} // namespace compaction
