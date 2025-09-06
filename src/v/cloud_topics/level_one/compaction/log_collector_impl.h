/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/level_one/compaction/log_collector.h"
#include "cluster/notification.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/topic_table.h"

namespace cloud_topics::l1 {

class compaction_scheduler;

class partition_leader_log_collector : public log_collector {
public:
    partition_leader_log_collector(
      compaction_scheduler* scheduler,
      ss::gate& gate,
      model::node_id self,
      ss::sharded<cluster::partition_leaders_table>* leaders,
      ss::sharded<cluster::topic_table>* topic_table)
      : log_collector(scheduler)
      , _gate(gate)
      , _self(self)
      , _leaders(leaders)
      , _topic_table(topic_table) {}

    ss::future<> start() final;
    ss::future<> stop() final;

private:
    ss::future<> on_leadership_change(model::ntp ntp, model::node_id leader);

    // A reference to the `_scheduler`'s `_gate`.
    ss::gate& _gate;
    model::node_id _self;
    cluster::notification_id_type _leader_notify_handle;

    ss::sharded<cluster::partition_leaders_table>* _leaders;
    ss::sharded<cluster::topic_table>* _topic_table;
};

} // namespace cloud_topics::l1
