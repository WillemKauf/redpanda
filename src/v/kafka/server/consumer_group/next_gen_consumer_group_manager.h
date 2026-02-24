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

#pragma once

#include "cluster/fwd.h"
#include "kafka/protocol/consumer_group_heartbeat.h"
#include "kafka/protocol/schemata/consumer_group_describe_response.h"
#include "kafka/server/consumer_group/coordinator.h"
#include "model/metadata.h"

#include <seastar/core/gate.hh>
#include <seastar/core/shared_ptr.hh>

#include "absl/container/node_hash_map.h"

namespace kafka::consumer_group {

/// Manages KIP-848 consumer group coordinators.
///
/// Owns the lifecycle of coordinator instances, one per __consumer_offsets
/// partition. group_manager delegates KIP-848 requests here.
class next_gen_consumer_group_manager {
public:
    next_gen_consumer_group_manager() = default;

    ss::future<> start();
    ss::future<> stop();

    /// Called when a __consumer_offsets partition is attached.
    void attach_partition(
      const model::ntp&, ss::lw_shared_ptr<cluster::partition>);

    /// Called when a __consumer_offsets partition is detached.
    ss::future<> detach_partition(const model::ntp&);

    /// KIP-848 ConsumerGroupHeartbeat
    ss::future<kafka::consumer_group_heartbeat_response>
    heartbeat(kafka::consumer_group_heartbeat_request&&);

    /// KIP-848 ConsumerGroupDescribe (single group)
    kafka::consumer_group_describe_described_group
    describe(const model::ntp&, const kafka::group_id&);

private:
    coordinator* get_or_create_coordinator(const model::ntp&);

    absl::node_hash_map<model::ntp, ss::lw_shared_ptr<cluster::partition>>
      _partitions;
    absl::node_hash_map<model::ntp, std::unique_ptr<coordinator>>
      _coordinators;
    ss::gate _gate;
};

} // namespace kafka::consumer_group
