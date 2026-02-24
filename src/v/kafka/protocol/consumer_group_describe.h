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
#include "kafka/protocol/schemata/consumer_group_describe_request.h"
#include "kafka/protocol/schemata/consumer_group_describe_response.h"

#include <seastar/core/future.hh>

namespace kafka {

struct consumer_group_describe_request final {
    using api_type = consumer_group_describe_api;

    consumer_group_describe_request_data data;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(protocol::decoder& reader, api_version version) {
        data.decode(reader, version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const consumer_group_describe_request& r) {
        return os << r.data;
    }
};

struct consumer_group_describe_response final {
    using api_type = consumer_group_describe_api;

    consumer_group_describe_response_data data;

    consumer_group_describe_response() = default;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const consumer_group_describe_response& r) {
        return os << r.data;
    }
};

} // namespace kafka
