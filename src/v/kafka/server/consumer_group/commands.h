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
#include "serde/rw/rw.h"
#include "utils/named_type.h"

namespace kafka::consumer_group {

using cmd_key = named_type<uint8_t, struct cmd_key_tag>;

/// Command 0: Update group-level metadata (epoch, state, assignor).
struct update_group_metadata_cmd {
    static constexpr cmd_key key{0};

    struct value
      : serde::envelope<value, serde::version<0>, serde::compat_version<0>> {
        kafka::group_id group_id;
        group_epoch epoch{0};
        group_state state{group_state::empty};
        ss::sstring assignor_name;

        auto serde_fields() {
            return std::tie(group_id, epoch, state, assignor_name);
        }

        friend bool operator==(const value&, const value&) = default;
    };
};

/// Command 1: Upsert (create or update) a member.
struct upsert_member_cmd {
    static constexpr cmd_key key{1};

    struct value
      : serde::envelope<value, serde::version<0>, serde::compat_version<0>> {
        kafka::group_id group_id;
        member member_data;

        auto serde_fields() { return std::tie(group_id, member_data); }

        friend bool operator==(const value&, const value&) = default;
    };
};

/// Command 2: Remove a member from the group.
struct remove_member_cmd {
    static constexpr cmd_key key{2};

    struct value
      : serde::envelope<value, serde::version<0>, serde::compat_version<0>> {
        kafka::group_id group_id;
        kafka::member_id member_id;

        auto serde_fields() { return std::tie(group_id, member_id); }

        friend bool operator==(const value&, const value&) = default;
    };
};

/// Command 3: Update the topic metadata tracked by the group.
struct update_topic_metadata_cmd {
    static constexpr cmd_key key{3};

    struct value
      : serde::envelope<value, serde::version<0>, serde::compat_version<0>> {
        kafka::group_id group_id;
        chunked_vector<topic_metadata> topics;

        auto serde_fields() { return std::tie(group_id, topics); }

        friend bool operator==(const value&, const value&) = default;
    };
};

/// Command 4: Set the target assignment for the group.
struct set_target_assignment_cmd {
    static constexpr cmd_key key{4};

    struct value
      : serde::envelope<value, serde::version<0>, serde::compat_version<0>> {
        kafka::group_id group_id;
        target_assignment assignment;

        auto serde_fields() { return std::tie(group_id, assignment); }

        friend bool operator==(const value&, const value&) = default;
    };
};

/// Command 5: Update a member's assignment state and partitions.
struct update_member_assignment_cmd {
    static constexpr cmd_key key{5};

    struct value
      : serde::envelope<value, serde::version<0>, serde::compat_version<0>> {
        kafka::group_id group_id;
        kafka::member_id member_id;
        member_epoch epoch{0};
        member_assignment_state state{member_assignment_state::stable};
        chunked_vector<topic_partitions> assigned_partitions;
        chunked_vector<topic_partitions> revoking_partitions;
        chunked_vector<topic_partitions> pending_partitions;

        auto serde_fields() {
            return std::tie(
              group_id,
              member_id,
              epoch,
              state,
              assigned_partitions,
              revoking_partitions,
              pending_partitions);
        }

        friend bool operator==(const value&, const value&) = default;
    };
};

/// Command 6: Commit offsets for a group.
struct commit_offset_cmd {
    static constexpr cmd_key key{6};

    struct value
      : serde::envelope<value, serde::version<0>, serde::compat_version<0>> {
        kafka::group_id group_id;
        chunked_vector<committed_offset> offsets;

        auto serde_fields() { return std::tie(group_id, offsets); }

        friend bool operator==(const value&, const value&) = default;
    };
};

/// Command 7: Delete the group entirely.
struct delete_group_cmd {
    static constexpr cmd_key key{7};

    struct value
      : serde::envelope<value, serde::version<0>, serde::compat_version<0>> {
        kafka::group_id group_id;

        auto serde_fields() { return std::tie(group_id); }

        friend bool operator==(const value&, const value&) = default;
    };
};

} // namespace kafka::consumer_group
