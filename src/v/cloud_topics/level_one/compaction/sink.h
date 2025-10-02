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

    ss::future<ss::stop_iteration>
    operator()(model::record_batch, model::compression) final;

    ss::future<> finalize() final;

    void set_range_has_tombstones() { _range_has_tombstones = true; }

private:
    // Returns `true` if the current object represented by
    // `_active_staging_file` and `_builder` should be rolled (i.e the existing
    // L1 is closed and pushed to `_closed_staging_files` and
    // `_closed_object_infos`, and a new `_active_staging_file` and `_builder`
    // are started)
    bool needs_roll() const;

    // Closes the existing L1 represented by `_active_staging_file` and
    // `_builder`, pushing it back to `_closed_staging_files_and_md_infos`, and
    // then reassigning `_active_staging_file` and `_builder` to construct a new
    // L1 object
    ss::future<> roll(bool);

    // Calls `roll()` iff `needs_roll() == true`.
    ss::future<> maybe_roll();

    // Returns `true` if the compaction update built up so far in
    // `_closed_staging_files_and_infos` can be pushed to the
    // `_committer`. The `_committer` will still decide (based on its
    // `committing_policy`) when these updates should be finalized and committed
    // to the `metastore` and cloud storage- this function only decides when the
    // `sink` has done enough checkpointable compaction work worth pushing to
    // the `_committer` as an update.
    bool needs_pushing() const;

    // Pushes the current compaction update composed of the objects in
    // `_closed_staging_files_and_infos` to the `_committer`. `roll()` should be
    // called before calling `push_update()`.
    void push_update();

    // Calls `push_update()` iff `needs_pushing() == true` and
    // `_closed_staging_files_and_md_infos` is not empty. For that reason,
    // `maybe_roll()` should be called before calling `maybe_push_update()`.
    void maybe_push_update();

private:
    model::topic_id_partition _tp;

    // Offset ranges for the contained `topic_id_partition` obtained from the
    // metastore.
    using interval_vec = chunked_vector<offset_interval_set::interval>;
    const interval_vec& _dirty_range_intervals;
    const offset_interval_set& _removable_tombstone_ranges;

    // Iterator used to track which dirty range is being compacted, which points
    // into the above vector `_dirty_range_intervals`.
    interval_vec::const_iterator _dirty_range_it;

    io* _io;
    compaction_committer* _committer;

    const object_builder::options _opts;

    std::unique_ptr<staging_file> _active_staging_file{nullptr};
    // Guaranteed to have a value iff _active_staging_file.
    std::unique_ptr<object_builder> _builder{nullptr};
    chunked_vector<staging_file_and_md_info>
      _closed_staging_files_and_md_infos{};
    bool _range_has_tombstones{false};

    kafka::offset _max_batch_offset;
};

} // namespace cloud_topics::l1
