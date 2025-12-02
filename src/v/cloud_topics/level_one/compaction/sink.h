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

#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/compaction/committer.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "compaction/reducer.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "model/timestamp.h"

namespace cloud_topics::l1 {

class compaction_sink : public compaction::sliding_window_reducer::sink {
public:
    compaction_sink(
      model::topic_id_partition,
      const chunked_vector<offset_interval_set::interval>&,
      const offset_interval_set&,
      l1::io*,
      compaction_committer*,
      object_builder::options = {});

    ss::future<bool>
    initialize(compaction::sliding_window_reducer::source&) final;

    ss::future<ss::stop_iteration>
    operator()(model::record_batch, model::compression) final;

    ss::future<> finalize() final;

private:
    // Returns `true` if the current object represented by
    // `_active_staging_file` and `_builder` should be rolled.
    bool needs_roll() const;

    // Pushes the current object represented by `_active_staging_file` and
    // `_builder` to the `_committer`. Leaves `_active_staging_file` and
    // `_builder` as `nullptr`.
    ss::future<> commit_update_and_roll();

    // Closes the existing L1 represented by `_active_staging_file` and
    // `_builder`, pushing it to the current compaction job in the `_committer`,
    // and then reassigning `_active_staging_file` and `_builder` to construct a
    // new L1 object
    ss::future<> roll(bool);

    // Calls `roll()` iff `needs_roll() == true`.
    ss::future<> maybe_roll();

private:
    model::topic_id_partition _tp;

    // Offset ranges for the contained `topic_id_partition` obtained from the
    // metastore.
    using interval_vec = chunked_vector<offset_interval_set::interval>;
    const interval_vec& _dirty_range_intervals;
    const offset_interval_set& _removable_tombstone_ranges;

    io* _io;
    compaction_committer* _committer;

    const object_builder::options _opts;

    // The L1 object currently being built.
    std::unique_ptr<staging_file> _active_staging_file{nullptr};
    // Guaranteed to have a value iff _active_staging_file.
    std::unique_ptr<object_builder> _builder{nullptr};

    // The current `_extent`'s offsets. Batches received by the sink's
    // `operator()` are part of the extent that spans this range.
    kafka::offset _extent_base_offset;
    kafka::offset _extent_last_offset;

    // The last offset processed by the `sink`, inclusive. This is set when new
    // batches are passed to the `sink`, as well as when a new extent range is
    // considered.
    kafka::offset _last_processed_offset{};

    // The current active staging file's base offset. This is set when the first
    // extent is encountered, as well as when an L1 object is rolled (in which
    // case it should be set to the next offset after `_last_processed_offset`).
    kafka::offset _object_base_offset{};

    // The interval set that is populated by extents which have been read by the
    // `source` and written by the `sink`. This is important to know in order to
    // decide which dirty ranges and removable tombstone ranges have actually
    // been processed when finalizing the compaction job with the `_committer`.
    offset_interval_set _processed_extents;

    // Dirty ranges returned by the `metastore` that were indexed during
    // `map_deduplication_iteration`.
    chunked_vector<metastore::compaction_update::cleaned_range>
      _new_cleaned_ranges;
};

} // namespace cloud_topics::l1
