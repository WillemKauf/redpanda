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

#include "container/chunked_vector.h"
#include "kafka/server/consumer_group/types.h"

namespace kafka::consumer_group {

/// Computes a target assignment for all members in a consumer group.
///
/// The uniform assignor distributes partitions as evenly as possible
/// across group members, preferring to keep existing assignments stable.
target_assignment compute_target_assignment(
  assignment_epoch epoch,
  const absl::node_hash_map<kafka::member_id, member>& members,
  const chunked_vector<topic_metadata>& topics);

} // namespace kafka::consumer_group
