// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/consumer_group/next_gen_consumer_group_manager.h"

#include "cluster/partition.h"
#include "kafka/protocol/errors.h"
#include "kafka/server/consumer_group/consumer_group_stm.h"
#include "kafka/server/logger.h"
#include "raft/consensus.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>

namespace kafka::consumer_group {

ss::future<> next_gen_consumer_group_manager::start() { co_return; }

ss::future<> next_gen_consumer_group_manager::stop() {
    for (auto& [_, coord] : _coordinators) {
        co_await coord->stop();
    }
    co_await _gate.close();
}

void next_gen_consumer_group_manager::attach_partition(
  const model::ntp& ntp, ss::lw_shared_ptr<cluster::partition> partition) {
    _partitions.emplace(ntp, std::move(partition));
}

ss::future<>
next_gen_consumer_group_manager::detach_partition(const model::ntp& ntp) {
    auto coord_it = _coordinators.find(ntp);
    if (coord_it != _coordinators.end()) {
        co_await coord_it->second->stop();
        _coordinators.erase(coord_it);
    }
    _partitions.erase(ntp);
}

coordinator*
next_gen_consumer_group_manager::get_or_create_coordinator(
  const model::ntp& ntp) {
    auto it = _coordinators.find(ntp);
    if (it != _coordinators.end()) {
        return it->second.get();
    }

    auto partition_it = _partitions.find(ntp);
    if (partition_it == _partitions.end() || !partition_it->second) {
        return nullptr;
    }

    auto& partition = partition_it->second;
    auto raft = partition->raft();
    if (!raft->is_leader() || !raft->stm_manager()) {
        return nullptr;
    }

    auto stm = raft->stm_manager()->get<consumer_group_stm>();
    if (!stm) {
        return nullptr;
    }

    auto coordinator = std::make_unique<consumer_group::coordinator>(
      *stm, std::chrono::milliseconds(5000), std::chrono::milliseconds(45000));
    coordinator->start();

    auto* ptr = coordinator.get();
    _coordinators.emplace(ntp, std::move(coordinator));
    return ptr;
}

ss::future<kafka::consumer_group_heartbeat_response>
next_gen_consumer_group_manager::heartbeat(
  kafka::consumer_group_heartbeat_request&& request) {
    auto* coord = get_or_create_coordinator(request.ntp);
    if (!coord) {
        co_return kafka::consumer_group_heartbeat_response(
          kafka::error_code::not_coordinator);
    }

    auto resp_data = co_await coord->heartbeat(std::move(request.data));
    kafka::consumer_group_heartbeat_response response;
    response.data = std::move(resp_data);
    co_return response;
}

kafka::consumer_group_describe_described_group
next_gen_consumer_group_manager::describe(
  const model::ntp& ntp, const kafka::group_id& group) {
    auto* coord = get_or_create_coordinator(ntp);
    if (!coord) {
        kafka::consumer_group_describe_described_group resp;
        resp.group_id = group;
        resp.error_code = kafka::error_code::not_coordinator;
        return resp;
    }

    return coord->describe(group);
}

} // namespace kafka::consumer_group
