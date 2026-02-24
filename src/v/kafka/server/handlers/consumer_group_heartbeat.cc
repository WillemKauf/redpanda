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
#include "kafka/server/handlers/consumer_group_heartbeat.h"

#include "kafka/protocol/consumer_group_heartbeat.h"
#include "kafka/server/group_router.h"
#include "kafka/server/handlers/handler_interface.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"

namespace kafka {

template<>
ss::future<response_ptr> consumer_group_heartbeat_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    consumer_group_heartbeat_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    if (unlikely(ctx.recovery_mode_enabled())) {
        co_return co_await ctx.respond(
          consumer_group_heartbeat_response(error_code::policy_violation));
    }

    auto authz = ctx.authorized(
      security::acl_operation::read,
      group_id(request.data.group_id));

    if (!ctx.audit()) {
        co_return co_await ctx.respond(
          consumer_group_heartbeat_response(error_code::broker_not_available));
    }

    if (!authz) {
        co_return co_await ctx.respond(consumer_group_heartbeat_response(
          error_code::group_authorization_failed));
    }

    auto resp = co_await ctx.groups().consumer_group_heartbeat(
      std::move(request));
    co_return co_await ctx.respond(std::move(resp));
}

} // namespace kafka
