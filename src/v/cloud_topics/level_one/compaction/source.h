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

#include "compaction/reducer.h"

namespace cloud_topics::l1 {

class compaction_source : public compaction::sliding_window_reducer::source {
public:
    compaction_source(model::ntp ntp, ss::abort_source& as);
    ss::future<> initialize() final;
    ss::future<ss::stop_iteration> map_building_iteration() final;
    ss::future<ss::stop_iteration>
    deduplication_iteration(compaction::sliding_window_reducer::sink&) final;

private:
    model::ntp _ntp;
    ss::abort_source& _as;
};

} // namespace cloud_topics::l1
