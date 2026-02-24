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

#include "kafka/server/consumer_group/types.h"

#include <ostream>

namespace kafka::consumer_group {

std::ostream& operator<<(std::ostream& os, group_state s) {
    switch (s) {
    case group_state::empty:
        return os << "empty";
    case group_state::assigning:
        return os << "assigning";
    case group_state::reconciling:
        return os << "reconciling";
    case group_state::stable:
        return os << "stable";
    case group_state::dead:
        return os << "dead";
    }
    return os << "unknown(" << static_cast<int>(s) << ")";
}

std::ostream& operator<<(std::ostream& os, member_assignment_state s) {
    switch (s) {
    case member_assignment_state::stable:
        return os << "stable";
    case member_assignment_state::revoking:
        return os << "revoking";
    case member_assignment_state::assigning:
        return os << "assigning";
    }
    return os << "unknown(" << static_cast<int>(s) << ")";
}

} // namespace kafka::consumer_group
