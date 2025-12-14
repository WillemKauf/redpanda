/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/metastore/extent_metadata_reader.h"

#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/level_one/metastore/retry.h"

namespace cloud_topics::l1 {

extent_metadata_reader::extent_metadata_reader(
  metastore* metastore,
  model::topic_id_partition tp,
  kafka::offset min_offset,
  kafka::offset max_offset,
  iteration_direction iter_dir,
  ss::abort_source& as)
  : _metastore(metastore)
  , _tp(std::move(tp))
  , _min_offset(min_offset)
  , _max_offset(max_offset)
  , _iter_dir(iter_dir)
  , _as(as) {}

ss::future<std::expected<metastore::extent_metadata_response, metastore::errc>>
extent_metadata_reader::fetch_extents(kafka::offset next_fetch_offset) {
    vassert(_metastore, "metastore has no value");
    switch (_iter_dir) {
    case iteration_direction::forward:
        return retry_metastore_op_with_default_rtc(
          [this, next_fetch_offset] {
              return _metastore->get_extent_metadata_ge(
                _tp, next_fetch_offset, _max_offset, num_extents_per_request);
          },
          _as);
    case iteration_direction::backward:
        return retry_metastore_op_with_default_rtc(
          [this, next_fetch_offset] {
              return _metastore->get_extent_metadata_le(
                _tp, _min_offset, next_fetch_offset, num_extents_per_request);
          },
          _as);
    }
}

extent_metadata_reader::extent_metadata_generator
extent_metadata_reader::generator() {
    std::optional<kafka::offset> continuation_offset
      = _iter_dir == iteration_direction::forward ? _min_offset : _max_offset;
    while (continuation_offset.has_value()) {
        auto extent_md_res = co_await fetch_extents(
          continuation_offset.value());

        if (!extent_md_res.has_value()) {
            // Return the error to the user. Allow them to decide what
            // to do with the iteration.
            co_yield std::unexpected(extent_md_res.error());
        } else {
            // Yield extents.
            auto extents = std::move(extent_md_res->extents);
            continuation_offset = std::move(extent_md_res->continuation_offset);

            for (const auto& extent : extents) {
                co_yield extent;
            }
        }
    }
}

} // namespace cloud_topics::l1
