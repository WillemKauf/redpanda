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
#include "base/seastarx.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/consumer_group_heartbeat_request.h"
#include "kafka/protocol/schemata/consumer_group_heartbeat_response.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>

namespace kafka {

struct consumer_group_heartbeat_request final {
    using api_type = consumer_group_heartbeat_api;

    consumer_group_heartbeat_request_data data;

    // set during request processing after mapping group to ntp
    model::ntp ntp;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(protocol::decoder& reader, api_version version) {
        data.decode(reader, version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const consumer_group_heartbeat_request& r) {
        return os << r.data;
    }
};

struct consumer_group_heartbeat_response final {
    using api_type = consumer_group_heartbeat_api;

    consumer_group_heartbeat_response_data data;

    consumer_group_heartbeat_response() = default;

    explicit consumer_group_heartbeat_response(error_code error)
      : data({
          .error_code = error,
        }) {}

    consumer_group_heartbeat_response(
      const consumer_group_heartbeat_request&, error_code error)
      : consumer_group_heartbeat_response(error) {}

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    friend std::ostream& operator<<(
      std::ostream& os, const consumer_group_heartbeat_response& r) {
        return os << r.data;
    }
};

inline ss::future<consumer_group_heartbeat_response>
make_consumer_group_heartbeat_error(error_code error) {
    return ss::make_ready_future<consumer_group_heartbeat_response>(error);
}

} // namespace kafka
