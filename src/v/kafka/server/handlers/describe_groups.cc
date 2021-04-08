// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/describe_groups.h"

#include "kafka/protocol/errors.h"
#include "kafka/server/group_manager.h"
#include "kafka/server/group_router.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "model/namespace.h"
#include "resource_mgmt/io_priority.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/when_all.hh>

namespace kafka {

void describe_groups_response::encode(
  const request_context& ctx, response& resp) {
    data.encode(resp.writer(), ctx.header().version);
}


template<>
ss::future<response_ptr> describe_groups_handler::handle(
  request_context ctx, ss::smp_service_group ssg) {
    describe_groups_request request;
    request.decode(ctx.reader(), ctx.header().version);
    klog.trace("Handling request {}", request);

    std::vector<ss::future<described_group>> described;

    for (auto& group_id : request.data.groups) {
        described.push_back(ctx.groups().describe_group(std::move(group_id)));
    }

    describe_groups_response response;
    response.data.groups = co_await ss::when_all_succeed(
      described.begin(), described.end());

    co_return co_await ctx.respond(std::move(response));
}

} // namespace kafka
