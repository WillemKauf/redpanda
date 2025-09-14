/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/sink.h"

#include "bytes/iostream.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/compaction/logger.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "compaction/reducer.h"
#include "model/batch_compression.h"
#include "model/compression.h"

#include <seastar/coroutine/as_future.hh>

namespace cloud_topics::l1 {

compaction_sink::compaction_sink(
  io* io,
  compaction_committer* committer,
  model::topic_id_partition tp,
  object_builder::options opts)
  : _io(io)
  , _committer(committer)
  , _tp(tp)
  , _opts(opts) {}

bool compaction_sink::needs_roll() const {
    // TODO: This needs to consider L1 object size and what-not eventually.
    return !_active_staging_file;
}

ss::future<> compaction_sink::commit_update() {
    if (!_active_staging_file) {
        co_return;
    }

    auto active_staging_file = std::exchange(_active_staging_file, nullptr);
    auto builder = std::exchange(_builder, nullptr);

    auto object_info = co_await builder->finish().finally(
      [&builder] { return builder->close(); });

    auto out = object_output_t{
      .tp = _tp,
      .info = std::move(object_info),
      .staging_file = std::move(active_staging_file)};
    _committer->push_update(std::move(out));
}

ss::future<> compaction_sink::maybe_roll() {
    if (!needs_roll()) {
        co_return;
    }

    co_await commit_update();

    auto staging_file_fut = co_await ss::coroutine::as_future(
      _io->create_tmp_file());

    if (staging_file_fut.failed()) {
        auto ex = staging_file_fut.get_exception();
        vlog(compaction_log.error, "Exception creating staging file: {}", ex);
        std::rethrow_exception(ex);
    }
    auto staging_file_result = staging_file_fut.get();

    _active_staging_file = std::move(staging_file_result).value();
    auto output_stream = co_await _active_staging_file->output_stream();

    _builder = object_builder::create(std::move(output_stream), _opts);

    co_await _builder->start_partition(_tp);

    co_return;
}

ss::future<ss::stop_iteration>
compaction_sink::operator()(model::record_batch b, model::compression c) {
    co_await maybe_roll();
    if (c != model::compression::none) {
        b = co_await model::compress_batch(c, std::move(b));
    }
    co_await _builder->add_batch(std::move(b));
    co_return ss::stop_iteration::no;
}

ss::future<> compaction_sink::finalize() { co_await commit_update(); }

} // namespace cloud_topics::l1
