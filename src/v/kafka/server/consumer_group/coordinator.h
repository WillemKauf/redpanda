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

#include "kafka/protocol/schemata/consumer_group_describe_response.h"
#include "kafka/protocol/schemata/consumer_group_heartbeat_request.h"
#include "kafka/protocol/schemata/consumer_group_heartbeat_response.h"
#include "kafka/server/consumer_group/consumer_group_stm.h"
#include "kafka/server/consumer_group/types.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/timer.hh>

#include "absl/container/node_hash_map.h"

namespace kafka::consumer_group {

/// Coordinator for KIP-848 consumer groups.
///
/// Manages the business logic for consumer group heartbeats, member
/// lifecycle, assignment, reconciliation, and offset management.
/// All mutations go through the STM via replicate_and_wait().
class coordinator {
public:
    coordinator(
      consumer_group_stm& stm,
      std::chrono::milliseconds heartbeat_interval,
      std::chrono::milliseconds session_timeout);

    ~coordinator();

    /// Process a ConsumerGroupHeartbeat request.
    ss::future<kafka::consumer_group_heartbeat_response_data>
    heartbeat(kafka::consumer_group_heartbeat_request_data);

    /// Process a ConsumerGroupDescribe request for a single group.
    kafka::described_group describe(const kafka::group_id&);

    /// Start the session expiration timer.
    void start();

    /// Stop the coordinator and wait for pending operations.
    ss::future<> stop();

private:
    ss::future<kafka::consumer_group_heartbeat_response_data>
    handle_join(kafka::consumer_group_heartbeat_request_data);

    ss::future<kafka::consumer_group_heartbeat_response_data>
    handle_leave(kafka::consumer_group_heartbeat_request_data);

    ss::future<kafka::consumer_group_heartbeat_response_data>
    handle_heartbeat(kafka::consumer_group_heartbeat_request_data);

    ss::future<std::error_code> run_assignor(const kafka::group_id&);

    kafka::consumer_group_heartbeat_response_data
    build_response(const kafka::group_id&, const member&);

    void expire_sessions();

    consumer_group_stm& _stm;
    std::chrono::milliseconds _heartbeat_interval;
    std::chrono::milliseconds _session_timeout;
    ss::gate _gate;
    ss::abort_source _as;
    ss::timer<ss::lowres_clock> _session_timer;
};

} // namespace kafka::consumer_group
