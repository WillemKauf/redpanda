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

#include "recovery_rpc_handler.h"

#include "cluster/controller_api.h"
#include "cluster/types.h"
#include "raft/recovery_rpc_types.h"
#include "rpc/types.h"

#include <seastar/coroutine/as_future.hh>

#include <optional>

namespace raft {

ss::future<reset_learner_state_reply> recovery_rpc_handler::reset_learner_state(
  reset_learner_state_request req, rpc::streaming_context&) {
    vlog(raftlog.debug, "Recieved request to remake partition {}", req.ntp);
    auto ec = co_await _api.local().remake_partition(req.ntp);
    if (ec) {
        vlog(
          raftlog.debug,
          "Error encountered while remaking partition {}: {}",
          req.ntp,
          ec);
        co_return reset_learner_state_reply{};
    }

    vlog(raftlog.debug, "Successfully remade partition {}", req.ntp);
    co_return reset_learner_state_reply{
      .success = reset_learner_state_reply::is_success::yes};
}

} // namespace raft
