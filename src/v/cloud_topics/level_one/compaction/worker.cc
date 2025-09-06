/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/worker.h"

#include "cloud_topics/level_one/compaction/logger.h"
#include "cloud_topics/level_one/compaction/sink.h"
#include "cloud_topics/level_one/compaction/source.h"
#include "compaction/reducer.h"
#include "model/fundamental.h"
#include "ssx/future-util.h"

#include <seastar/coroutine/as_future.hh>

namespace cloud_topics::l1 {

ss::future<> compaction_worker::compact(model::ntp ntp, ss::abort_source& as) {
    _state = compaction_job_state::running;
    _ntp = ntp;

    auto src = std::make_unique<compaction_source>(ntp, as, _state);
    auto sink = std::make_unique<compaction_sink>(ntp);
    auto reducer = compaction::sliding_window_reducer(
      std::move(src), std::move(sink));

    auto compact_fut = co_await ss::coroutine::as_future(
      std::move(reducer).run());

    if (compact_fut.failed()) {
        auto eptr = compact_fut.get_exception();
        auto log_lvl = ssx::is_shutdown_exception(eptr) ? ss::log_level::warn
                                                        : ss::log_level::debug;
        vlogl(
          compact_log,
          log_lvl,
          "Caught exception {} while compacting ntp {}.",
          eptr,
          ntp);
    }

    _state = compaction_job_state::idle;
    _ntp.reset();
}

void compaction_worker::request_stop_compact(model::ntp expected_ntp) {
    if (_ntp == expected_ntp && _state == compaction_job_state::running) {
        _state = compaction_job_state::stopped;
    }
}

} // namespace cloud_topics::l1
