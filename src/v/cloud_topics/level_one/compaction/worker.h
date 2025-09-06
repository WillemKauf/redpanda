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

#include "cloud_topics/level_one/compaction/source.h"

namespace cloud_topics::l1 {

class compaction_worker {
public:
    // Requests a compaction of the provided `ntp`.
    ss::future<> compact(model::ntp ntp, ss::abort_source& as);

    // Sets `_state = compaction_job_state::stopped` iff `expected_ntp == _ntp`.
    // It is up to users to respect this flag.
    void request_stop_compact(model::ntp expected_ntp);

    // Specifies which `ntp` is currently undergoing compaction on this
    // worker. Set iff `_state == compaction_job_state::running`.
    std::optional<model::ntp> _ntp;
    compaction_job_state _state;
};

} // namespace cloud_topics::l1
