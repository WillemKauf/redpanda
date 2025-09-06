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

#include "container/intrusive_list_helpers.h"
#include "model/fundamental.h"
#include "model/timestamp.h"

#include <seastar/core/gate.hh>

#include <memory>

namespace cloud_topics::l1 {

struct log_info {
    model::ntp ntp;
    bool must_compact;
    double dirty_ratio;
    model::timestamp earliest_dirty_ts;
};

struct log_compaction_meta {
    log_compaction_meta(model::ntp ntp)
      : ntp(std::move(ntp)) {}

    model::ntp ntp;
    ss::gate gate;
    intrusive_list_hook link;
};

using log_compaction_meta_ptr = std::unique_ptr<log_compaction_meta>;

struct log_compaction_meta_hash {
    using is_transparent = void;

    size_t
    operator()(const cloud_topics::l1::log_compaction_meta_ptr& m) const {
        return std::hash<model::ntp>{}(m->ntp);
    }

    size_t operator()(const model::ntp& ntp) const {
        return std::hash<model::ntp>{}(ntp);
    }
};

struct log_compaction_meta_eq {
    using is_transparent = void;

    bool operator()(
      const cloud_topics::l1::log_compaction_meta_ptr& lhs,
      const cloud_topics::l1::log_compaction_meta_ptr& rhs) const {
        return lhs->ntp == rhs->ntp;
    }

    bool operator()(
      const cloud_topics::l1::log_compaction_meta_ptr& lhs,
      const model::ntp& rhs) const {
        return lhs->ntp == rhs;
    }

    bool operator()(
      const model::ntp& lhs,
      const cloud_topics::l1::log_compaction_meta_ptr& rhs) const {
        return lhs == rhs->ntp;
    }
};

struct log_info_and_meta {
    log_info info;
    log_compaction_meta* meta;
};

} // namespace cloud_topics::l1
