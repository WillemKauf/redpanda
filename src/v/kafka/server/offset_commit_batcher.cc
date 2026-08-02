/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/server/offset_commit_batcher.h"

#include "cluster/partition.h"
#include "kafka/server/logger.h"
#include "raft/replicate.h"
#include "ssx/future-util.h"

namespace kafka {

offset_commit_batcher::offset_commit_batcher(
  ss::lw_shared_ptr<cluster::partition> p)
  : _partition(std::move(p)) {}

offset_commit_batcher::~offset_commit_batcher() = default;

namespace {
void append_records(
  cluster::simple_batch_builder& builder,
  chunked_vector<group_metadata_serializer::key_value> records) {
    for (auto& kv : records) {
        builder.add_raw_kv(std::move(kv.key), std::move(kv.value));
    }
}
} // namespace

offset_commit_batcher::stages offset_commit_batcher::replicate(
  model::term_id term,
  chunked_vector<group_metadata_serializer::key_value> records) {
    if (_in_flight < max_in_flight && !_pending) {
        return replicate_directly(term, std::move(records));
    }
    /*
     * term changes are rare (a coordinator leadership change) and the record
     * cap bounds the memory held by the pending batch; both cases bypass
     * coalescing and defer to raft's own backpressure.
     */
    if (
      _pending
      && (_pending->term != term || _pending->records + records.size() > max_pending_records)) {
        return replicate_directly(term, std::move(records));
    }
    if (!_pending) {
        _pending.emplace(term);
    }
    const auto n = records.size();
    append_records(_pending->builder, std::move(records));
    _pending->records += n;

    auto w = std::make_unique<waiter>();
    w->record_end = _pending->records;
    auto committed = w->committed.get_future();
    _pending->waiters.push_back(std::move(w));
    return {ss::now(), std::move(committed)};
}

offset_commit_batcher::stages offset_commit_batcher::replicate_directly(
  model::term_id term,
  chunked_vector<group_metadata_serializer::key_value> records) {
    cluster::simple_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));
    append_records(builder, std::move(records));

    ++_in_flight;
    auto stages = _partition->raft()->replicate_in_stages(
      chunked_vector<model::record_batch>::single(std::move(builder).build()),
      raft::replicate_options(raft::consistency_level::quorum_ack, term));

    auto committed = stages.replicate_finished.then_wrapped(
      [this,
       self = shared_from_this()](ss::future<result<raft::replicate_result>> f)
        -> ss::future<result<model::offset>> {
          --_in_flight;
          maybe_flush();
          if (f.failed()) {
              return ss::make_exception_future<result<model::offset>>(
                f.get_exception());
          }
          auto r = f.get();
          if (!r) {
              return ss::make_ready_future<result<model::offset>>(r.error());
          }
          return ss::make_ready_future<result<model::offset>>(
            r.value().last_offset);
      });
    return {std::move(stages.request_enqueued), std::move(committed)};
}

void offset_commit_batcher::maybe_flush() {
    if (!_pending || _in_flight >= max_in_flight) {
        return;
    }
    auto pending = std::exchange(_pending, std::nullopt);
    vlog(
      cg_klog.trace,
      "flushing {} coalesced offset commits ({} records) as one batch on {}",
      pending->waiters.size(),
      pending->records,
      _partition->ntp());

    ++_in_flight;
    auto stages = _partition->raft()->replicate_in_stages(
      chunked_vector<model::record_batch>::single(
        std::move(pending->builder).build()),
      raft::replicate_options(
        raft::consistency_level::quorum_ack, pending->term));

    // callers of coalesced commits were already told their records were
    // accepted; any enqueue failure surfaces through replicate_finished
    ssx::background = stages.request_enqueued.handle_exception(
      [](const std::exception_ptr&) {});

    ssx::background = stages.replicate_finished.then_wrapped(
      [this,
       self = shared_from_this(),
       records = pending->records,
       waiters = std::move(pending->waiters)](
        ss::future<result<raft::replicate_result>> f) mutable {
          --_in_flight;
          maybe_flush();
          if (f.failed()) {
              auto e = f.get_exception();
              for (auto& w : waiters) {
                  w->committed.set_exception(e);
              }
              return;
          }
          auto r = f.get();
          if (!r) {
              for (auto& w : waiters) {
                  w->committed.set_value(r.error());
              }
              return;
          }
          const auto base = r.value().last_offset()
                            - static_cast<int64_t>(records) + 1;
          for (auto& w : waiters) {
              w->committed.set_value(
                model::offset(base + static_cast<int64_t>(w->record_end) - 1));
          }
      });
}

} // namespace kafka
