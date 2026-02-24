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
#include "kafka/server/handlers/consumer_group_describe.h"

#include "kafka/protocol/consumer_group_describe.h"
#include "kafka/server/group_router.h"
#include "kafka/server/handlers/handler_interface.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"

namespace kafka {

template<>
ss::future<response_ptr> consumer_group_describe_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    consumer_group_describe_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    if (unlikely(ctx.recovery_mode_enabled())) {
        consumer_group_describe_response response;
        for (const auto& gid : request.data.group_ids) {
            kafka::consumer_group_describe_described_group grp;
            grp.group_id = gid;
            grp.error_code = error_code::policy_violation;
            response.data.groups.push_back(std::move(grp));
        }
        co_return co_await ctx.respond(std::move(response));
    }

    if (!ctx.audit()) {
        consumer_group_describe_response response;
        for (const auto& gid : request.data.group_ids) {
            kafka::consumer_group_describe_described_group grp;
            grp.group_id = gid;
            grp.error_code = error_code::broker_not_available;
            response.data.groups.push_back(std::move(grp));
        }
        co_return co_await ctx.respond(std::move(response));
    }

    consumer_group_describe_response response;
    for (const auto& gid : request.data.group_ids) {
        auto authz = ctx.authorized(
          security::acl_operation::describe, group_id(gid));
        if (!authz) {
            kafka::consumer_group_describe_described_group grp;
            grp.group_id = gid;
            grp.error_code = error_code::group_authorization_failed;
            response.data.groups.push_back(std::move(grp));
            continue;
        }

        auto desc = co_await ctx.groups().consumer_group_describe(
          group_id(gid));
        response.data.groups.push_back(std::move(desc));
    }

    co_return co_await ctx.respond(std::move(response));
}

} // namespace kafka
