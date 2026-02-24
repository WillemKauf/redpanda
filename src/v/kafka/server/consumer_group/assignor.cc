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

#include "kafka/server/consumer_group/assignor.h"

#include <algorithm>

namespace kafka::consumer_group {

target_assignment compute_target_assignment(
  assignment_epoch epoch,
  const absl::node_hash_map<kafka::member_id, member>& members,
  const chunked_vector<topic_metadata>& topics) {
    target_assignment result;
    result.epoch = epoch;

    if (members.empty()) {
        return result;
    }

    // Collect all subscribing members in a stable order.
    chunked_vector<kafka::member_id> member_ids;
    member_ids.reserve(members.size());
    for (const auto& [mid, _] : members) {
        member_ids.push_back(mid);
    }
    std::sort(member_ids.begin(), member_ids.end());

    // Build per-member assignment maps.
    absl::node_hash_map<kafka::member_id, target_assignment_member>
      assignment_map;
    for (const auto& mid : member_ids) {
        auto& tam = assignment_map[mid];
        tam.member_id = mid;
    }

    // For each topic, distribute partitions round-robin across members
    // that subscribe to it.
    for (const auto& topic : topics) {
        // Collect members subscribed to this topic.
        chunked_vector<kafka::member_id> subscribers;
        for (const auto& mid : member_ids) {
            const auto& m = members.at(mid);
            for (const auto& tid : m.subscribed_topic_ids) {
                if (tid == topic.topic_id) {
                    subscribers.push_back(mid);
                    break;
                }
            }
        }

        if (subscribers.empty()) {
            continue;
        }

        // Distribute partitions round-robin.
        for (int32_t p = 0; p < topic.num_partitions; ++p) {
            const auto& target_member
              = subscribers[p % subscribers.size()];
            auto& tam = assignment_map[target_member];

            // Find or create topic_partitions entry.
            topic_partitions* tp_entry = nullptr;
            for (auto& tp : tam.partitions) {
                if (tp.topic_id == topic.topic_id) {
                    tp_entry = &tp;
                    break;
                }
            }
            if (!tp_entry) {
                tam.partitions.push_back(
                  topic_partitions{.topic_id = topic.topic_id});
                tp_entry = &tam.partitions.back();
            }
            tp_entry->partitions.push_back(model::partition_id{p});
        }
    }

    // Convert map to result vector.
    for (auto& mid : member_ids) {
        result.members.push_back(std::move(assignment_map[mid]));
    }

    return result;
}

} // namespace kafka::consumer_group
