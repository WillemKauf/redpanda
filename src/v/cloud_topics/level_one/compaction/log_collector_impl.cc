/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/log_collector_impl.h"

#include "cloud_topics/level_one/compaction/scheduler.h"
#include "cluster/partition.h"
#include "cluster/partition_manager.h"
#include "cluster/topic_configuration.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "ssx/future-util.h"

namespace cloud_topics::l1 {

ss::future<> partition_leader_log_collector::start() {
    _leader_notify_handle
      = _leaders->local().register_leadership_change_notification(
        [this](const model::ntp& ntp, model::term_id, model::node_id leader) {
            ssx::spawn_with_gate(_gate, [this, ntp = std::move(ntp), leader] {
                return on_leadership_change(std::move(ntp), leader);
            });
        });
    co_return;
}

ss::future<> partition_leader_log_collector::stop() {
    _leaders->local().unregister_leadership_change_notification(
      _leader_notify_handle);
    co_return;
}

ss::future<> partition_leader_log_collector::on_leadership_change(
  model::ntp ntp, model::node_id leader) {
    auto topic_cfg_opt = _topic_table->local().get_topic_cfg(
      model::topic_namespace_view{ntp});
    if (!topic_cfg_opt.has_value()) {
        co_return;
    }

    cluster::topic_configuration& topic_cfg = topic_cfg_opt.value();

    auto is_compacted_cloud_topic = topic_cfg.is_compacted()
                                    && topic_cfg.is_cloud_topic();

    if (!is_compacted_cloud_topic) {
        co_return;
    }

    auto is_managed = _scheduler->is_managed(ntp);
    auto is_leader = leader == _self;

    if (is_leader && !is_managed) {
        _scheduler->manage_partition(ntp);
    }

    if (!is_leader && is_managed) {
        co_await _scheduler->unmanage_partition(ntp);
    }
}

} // namespace cloud_topics::l1
