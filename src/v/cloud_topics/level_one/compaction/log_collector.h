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

#include "base/seastarx.h"

#include <seastar/core/future.hh>

namespace cloud_topics::l1 {

class compaction_scheduler;

class log_collector {
public:
    log_collector(compaction_scheduler* scheduler)
      : _scheduler(scheduler) {}

    virtual ~log_collector() noexcept = default;

    virtual ss::future<> start() = 0;
    virtual ss::future<> stop() = 0;

protected:
    compaction_scheduler* _scheduler;
};

} // namespace cloud_topics::l1
