/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "base/seastarx.h"

#include <cstdint>

namespace kafka {

/// Lightweight probe for KIP-848 consumer group metrics.
///
/// Tracks aggregate statistics across all consumer groups on a shard.
/// Intended to be held by group_manager.
class consumer_group_probe {
public:
    void group_created() { ++_groups_total; }
    void group_deleted() { --_groups_total; }
    void member_joined() { ++_members_total; }
    void member_left() {
        if (_members_total > 0) {
            --_members_total;
        }
    }
    void rebalance_triggered() { ++_rebalances_total; }
    void heartbeat_received() { ++_heartbeats_total; }

    int64_t groups_total() const { return _groups_total; }
    int64_t members_total() const { return _members_total; }
    uint64_t rebalances_total() const { return _rebalances_total; }
    uint64_t heartbeats_total() const { return _heartbeats_total; }

private:
    int64_t _groups_total{0};
    int64_t _members_total{0};
    uint64_t _rebalances_total{0};
    uint64_t _heartbeats_total{0};
};

} // namespace kafka
