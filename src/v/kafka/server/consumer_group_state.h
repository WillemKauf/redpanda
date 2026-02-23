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

#include <fmt/format.h>

#include <cstdint>
#include <string_view>

namespace kafka {

/// KIP-848: Consumer group states for the new consumer group protocol.
///
/// These states differ from classic group states:
/// - No preparing_rebalance or completing_rebalance states
/// - Server-side assignment means the group transitions through
///   assigning and reconciling states instead
enum class consumer_group_state : int8_t {
    /// The group currently has no members.
    empty,

    /// The group is computing a new target assignment.
    assigning,

    /// Members are converging toward the target assignment.
    reconciling,

    /// All members have reached the target assignment.
    stable,

    /// Transient state as the group is being removed.
    dead,
};

constexpr std::string_view
consumer_group_state_to_string(consumer_group_state s) {
    switch (s) {
    case consumer_group_state::empty:
        return "Empty";
    case consumer_group_state::assigning:
        return "Assigning";
    case consumer_group_state::reconciling:
        return "Reconciling";
    case consumer_group_state::stable:
        return "Stable";
    case consumer_group_state::dead:
        return "Dead";
    }
    __builtin_unreachable();
}

} // namespace kafka

template<>
struct fmt::formatter<kafka::consumer_group_state> final
  : fmt::formatter<std::string_view> {
    template<typename FormatContext>
    auto
    format(const kafka::consumer_group_state& s, FormatContext& ctx) const {
        return formatter<string_view>::format(
          kafka::consumer_group_state_to_string(s), ctx);
    }
};
