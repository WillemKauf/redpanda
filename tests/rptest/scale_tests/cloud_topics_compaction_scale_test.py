# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import matrix
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.tests.cloud_topics.e2e_test import (
    EndToEndCloudTopicsBase,
    EndToEndCloudTopicsCompactionBase,
)


class CloudTopicsCompactionScaleTest(EndToEndCloudTopicsCompactionBase):
    def __init__(self, test_context):
        extra_rp_conf = {
            "log_compaction_interval_ms": 4000,
        }
        super().__init__(test_context, extra_rp_conf)

        partition_count = test_context.injected_args["partition_count"]

        self.topics = (
            TopicSpec(
                name=EndToEndCloudTopicsBase.s3_topic_name,
                partition_count=partition_count,
                replication_factor=3,
                cleanup_policy=TopicSpec.CLEANUP_COMPACT,
                min_cleanable_dirty_ratio=0.0,
                delete_retention_ms=3000,
            ),
        )

        bytes_per_partition = 1 * 1024**3  # 1 GiB
        max_total_bytes = 100 * 1024**3  # 100 GiB
        total_bytes = min(bytes_per_partition * partition_count, max_total_bytes)
        self.num_rounds = 5

        bytes_per_round = total_bytes // self.num_rounds
        self.msg_size = 16 * 1024  # 16KiB
        self.msg_count = bytes_per_round // self.msg_size
        self.key_set_cardinality = 10000
        self.tombstone_probability = 0.4

    @cluster(num_nodes=4)
    @matrix(partition_count=[1, 2, 64, 288])
    def test_compact(self, partition_count):
        self.do_test_compact(num_rounds=self.num_rounds)
