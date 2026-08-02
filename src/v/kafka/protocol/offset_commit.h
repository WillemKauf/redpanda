/*
 * Copyright 2020 Redpanda Data, Inc.
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
#include "bytes/iobuf.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/offset_commit_request.h"
#include "kafka/protocol/schemata/offset_commit_response.h"
#include "model/fundamental.h"
#include "model/timestamp.h"

#include <seastar/core/future.hh>

namespace kafka {

struct offset_commit_request final {
    using api_type = offset_commit_api;

    offset_commit_request_data data;

    // set during request processing after mapping group to ntp
    model::ntp ntp;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(protocol::decoder& reader, api_version version) {
        data.decode(reader, version);
    }

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{}", data);
    }
};

struct offset_commit_response final {
    using api_type = offset_commit_api;

    offset_commit_response_data data;

    offset_commit_response() = default;

    offset_commit_response(
      const offset_commit_request& request, error_code error) {
        data.topics.reserve(request.data.topics.size());
        for (const auto& t : request.data.topics) {
            offset_commit_response_topic tmp{.name = t.name};
            append_partitions(tmp, t, error);
            data.topics.push_back(std::move(tmp));
        }
    }

    // move overload for the offset commit hot path: steals the topic names
    // from a request that is no longer needed instead of copying them
    offset_commit_response(offset_commit_request&& request, error_code error) {
        data.topics.reserve(request.data.topics.size());
        for (auto& t : request.data.topics) {
            offset_commit_response_topic tmp{.name = std::move(t.name)};
            append_partitions(tmp, t, error);
            data.topics.push_back(std::move(tmp));
        }
    }

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{}", data);
    }

private:
    static void append_partitions(
      offset_commit_response_topic& topic,
      const offset_commit_request_topic& t,
      error_code error) {
        topic.partitions.reserve(t.partitions.size());
        for (const auto& p : t.partitions) {
            topic.partitions.push_back({
              .partition_index = p.partition_index,
              .error_code = error,
            });
        }
    }
};

} // namespace kafka
