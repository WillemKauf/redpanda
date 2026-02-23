// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/consumer_group_assignor.h"

#include <algorithm>

namespace kafka {

const kafka::server_assignor uniform_assignor::_name{"uniform"};

const kafka::server_assignor& uniform_assignor::name() const { return _name; }

assignor_result uniform_assignor::assign(
  const std::vector<assignable_topic>& topics,
  const absl::node_hash_map<kafka::member_id, consumer_group_member_ptr>&
    members) {
    assignor_result result;

    // Initialize empty assignments for all members.
    for (const auto& [mid, _] : members) {
        result[mid] = {};
    }

    // For each topic, collect subscribers and distribute partitions evenly.
    for (const auto& topic : topics) {
        // Collect members subscribed to this topic.
        std::vector<kafka::member_id> subscribers;
        for (const auto& [mid, member] : members) {
            for (const auto& t : member->subscribed_topic_names()) {
                if (t == topic.name) {
                    subscribers.push_back(mid);
                    break;
                }
            }
        }

        if (subscribers.empty()) {
            continue;
        }

        // Sort for deterministic assignment.
        std::sort(subscribers.begin(), subscribers.end());

        // Round-robin distribute partitions across subscribers.
        for (int32_t p = 0; p < topic.partition_count; ++p) {
            auto& member_assignment
              = result[subscribers[p % subscribers.size()]];
            member_assignment[topic.id].push_back(model::partition_id(p));
        }
    }

    return result;
}

} // namespace kafka
