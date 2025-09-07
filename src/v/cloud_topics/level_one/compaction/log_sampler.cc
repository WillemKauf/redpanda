/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/log_sampler.h"

#include "container/chunked_vector.h"
#include "model/fundamental.h"

#include <seastar/core/coroutine.hh>

namespace cloud_topics::l1 {

ss::future<chunked_vector<log_info_and_meta>>
log_sampler::sample_logs(log_list_t& logs) const {
    chunked_vector<model::topic_id_partition> to_sample;
    for (const auto& log_meta : logs) {
        if (!log_meta.link.is_linked()) {
            continue;
        }
        to_sample.emplace_back(log_meta.ntp.tp);
    }

    chunked_vector<log_info_and_meta> ret;
    co_return ret;
}

std::unique_ptr<log_sampler> make_log_sampler(metastore* metastore) {
    return std::make_unique<log_sampler>(metastore);
}

} // namespace cloud_topics::l1
