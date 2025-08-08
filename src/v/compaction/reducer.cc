// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compaction/reducer.h"

namespace compaction {

ss::future<> compaction::reducer::run() && {
    // Step 0: Initialize source
    co_await _src->initialize_source();

    // Step 0.1: Check if we have anything from the source to compact. If not,
    // early return.
    if (_src->is_end_of_stream()) {
        co_await _src->end_of_stream();
        co_return;
    }

    co_await _src->initialize_sink(*_sink);

    // Step 1: Perform backward pass
    co_await ss::repeat([this]() { return _src->backward_pass_iteration(); });

    bool should_rewrite = co_await _src->end_of_stream();
    if (!should_rewrite) {
        co_return;
    }

    // Step 2: Perform forward pass.
    co_await ss::repeat(
      [this]() { return _src->forward_pass_iteration(*_sink); });

    // Done!
    co_await _sink->finalize();
}

} // namespace compaction
