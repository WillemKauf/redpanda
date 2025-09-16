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

#include "cloud_topics/level_one/compaction/logger.h"
#include "cloud_topics/level_one/compaction/scheduler.h"
#include "cluster/partition.h"
#include "cluster/partition_manager.h"
#include "cluster/topic_configuration.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "ssx/future-util.h"

namespace cloud_topics::l1 {

partition_leader_log_collector::partition_leader_log_collector(
  compaction_scheduler* scheduler,
  model::node_id self,
  ss::sharded<cluster::partition_leaders_table>* leaders,
  ss::sharded<cluster::topic_table>* topic_table)
  : log_collector(scheduler)
  , _self(self)
  , _leaders(leaders)
  , _topic_table(topic_table) {}

ss::future<> partition_leader_log_collector::start_collecting_logs() {
    _ntp_notify_handle = _topic_table->local().register_ntp_delta_notification(
      [this](cluster::topic_table::ntp_delta_range_t deltas) {
          for (const auto& delta : deltas) {
              on_ntp_change(delta);
          }
      });
    _leader_notify_handle
      = _leaders->local().register_leadership_change_notification(
        [this](const model::ntp& ntp, model::term_id, model::node_id leader) {
            on_leadership_change(std::move(ntp), leader);
        });
    co_return;
}

ss::future<> partition_leader_log_collector::stop_collecting_logs() {
    auto close_fut = _gate.close();
    _topic_table->local().unregister_ntp_delta_notification(_ntp_notify_handle);
    _leaders->local().unregister_leadership_change_notification(
      _leader_notify_handle);
    co_await std::move(close_fut);
    co_return;
}

void partition_leader_log_collector::on_ntp_change(
  cluster::topic_table::ntp_delta delta) {
    auto& ntp = delta.ntp;
    auto is_managed = _scheduler->is_managed(ntp);

    using delta_type = cluster::topic_table_ntp_delta_type;
    switch (delta.type) {
    case delta_type::removed: {
        // Partition/possibly topic was removed. Unmanage it if necessary.
        if (is_managed) {
            ssx::spawn_with_gate(_gate, [this, ntp = std::move(ntp)] {
                return _scheduler->unmanage_partition(
                  std::move(ntp), "Partition removed");
            });
        }
        return;
    }
    case delta_type::properties_updated: {
        auto topic_cfg_opt = _topic_table->local().get_topic_cfg(
          model::topic_namespace_view{ntp});
        if (!topic_cfg_opt.has_value()) {
            return;
        }

        auto& topic_cfg = topic_cfg_opt.value();

        auto is_compacted_cloud_topic = topic_cfg.is_compacted()
                                        && topic_cfg.is_cloud_topic();
        if (is_compacted_cloud_topic && !is_managed) {
            // This is likely an existing cloud topic which is now `compact`
            // enabled.
            auto tp_id = topic_cfg.tp_id;
            vassert(tp_id.has_value(), "Expected tp_id to have value.");
            auto tidp = model::topic_id_partition(
              tp_id.value(), ntp.tp.partition);

            _scheduler->manage_partition(ntp, tidp, "Enabled compaction");
        }

        if (!is_compacted_cloud_topic && is_managed) {
            // This is likely an existing cloud topic which is no longer
            // `compact` enabled.
            ssx::spawn_with_gate(_gate, [this, ntp = std::move(ntp)] {
                return _scheduler->unmanage_partition(
                  std::move(ntp), "Disabled compaction");
            });
        }
        return;
    }
    case delta_type::added:
        [[fallthrough]];
    case delta_type::replicas_updated:
        [[fallthrough]];
    case delta_type::disabled_flag_updated:
        return;
    }
}

void partition_leader_log_collector::on_leadership_change(
  model::ntp ntp, model::node_id leader) {
    auto topic_cfg_opt = _topic_table->local().get_topic_cfg(
      model::topic_namespace_view{ntp});
    if (!topic_cfg_opt.has_value()) {
        return;
    }

    auto& topic_cfg = topic_cfg_opt.value();

    auto is_compacted_cloud_topic = topic_cfg.is_compacted()
                                    && topic_cfg.is_cloud_topic();

    if (!is_compacted_cloud_topic) {
        return;
    }

    auto is_managed = _scheduler->is_managed(ntp);
    auto is_leader = leader == _self;

    if (is_leader && !is_managed) {
        auto tp_id = topic_cfg.tp_id;
        vassert(tp_id.has_value(), "Expected tp_id to have value.");
        auto tidp = model::topic_id_partition(tp_id.value(), ntp.tp.partition);

        _scheduler->manage_partition(ntp, tidp, "Became the leader");
    }

    if (!is_leader && is_managed) {
        ssx::spawn_with_gate(_gate, [this, ntp = std::move(ntp)] {
            return _scheduler->unmanage_partition(
              std::move(ntp), "Stepped down as leader");
        });
    }
}

std::unique_ptr<log_collector> make_default_log_collector(
  compaction_scheduler* scheduler, compaction_cluster_state state) {
    return std::make_unique<partition_leader_log_collector>(
      scheduler, state.self, state.leaders_table, state.topic_table);
}

} // namespace cloud_topics::l1
