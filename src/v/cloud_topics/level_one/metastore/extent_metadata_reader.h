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

#include "cloud_topics/level_one/metastore/metastore.h"
#include "model/fundamental.h"

#include <seastar/coroutine/generator.hh>

#include <expected>

namespace cloud_topics::l1 {

class extent_metadata_reader {
public:
    enum class iteration_direction { forward, backward };

    using extent_metadata_generator = ss::coroutine::experimental::generator<
      std::expected<metastore::extent_metadata, metastore::errc>>;

    // Arbitrary.
    static constexpr size_t num_extents_per_request = 100;

    extent_metadata_reader(
      metastore*,
      model::topic_id_partition,
      kafka::offset,
      kafka::offset,
      iteration_direction,
      ss::abort_source&);

    extent_metadata_generator generator();

private:
    ss::future<
      std::expected<metastore::extent_metadata_response, metastore::errc>>
      fetch_extents(kafka::offset);

    metastore* _metastore{nullptr};

    model::topic_id_partition _tp;
    kafka::offset _min_offset;
    kafka::offset _max_offset;
    iteration_direction _iter_dir;
    ss::abort_source& _as;
};

} // namespace cloud_topics::l1
