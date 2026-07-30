# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import cast

from ducktape.cluster.cluster import ClusterNode
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.kcl import KCL
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.services.redpanda_installer import (
    LATEST_RELEASED_MAJOR,
    RedpandaInstaller,
)
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.util import wait_until_result


class MultiTermSegmentsUpgradeTest(PreallocNodesTest):
    """
    Pre-upgrade (v1) segments hold one raft term each, and their
    configuration batches do not carry the term in the payload. After an
    upgrade, compaction stamps those configuration batches with their terms
    and merges the v1 segments across term boundaries into v2-named
    segments. Leader epoch (raft term) attribution must stay correct
    throughout: while the data is still v1, after stamping and merging, and
    across restarts (when terms are recovered from log data alone).
    """

    def __init__(self, test_context: TestContext):
        super().__init__(
            test_context=test_context,
            num_brokers=3,
            node_prealloc_count=1,
            extra_rp_conf={
                "log_compaction_interval_ms": 2000,
            },
        )
        self.installer = self.redpanda._installer

    def setUp(self):
        # the newest released version rather than
        # highest_from_prior_feature_version(HEAD): the latter cannot
        # resolve on local dev builds, which report version (0, 0, 0)
        self.installer.install(self.redpanda.nodes, LATEST_RELEASED_MAJOR)
        super().setUp()

    def _produce(self, topic: str, count: int):
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            topic,
            msg_size=512,
            msg_count=count,
            key_set_cardinality=100,
            custom_node=self.preallocated_nodes,
        )

    def _leader_epoch_and_hwm(self, rpk: RpkTool, topic: str) -> tuple[int, int]:
        def epoch_valid():
            partitions = list(rpk.describe_topic(topic))
            if (
                len(partitions) == 1
                and partitions[0].leader_epoch >= 0
                and partitions[0].high_watermark >= 0
            ):
                return True, (
                    partitions[0].leader_epoch,
                    partitions[0].high_watermark,
                )
            return False, None

        return wait_until_result(epoch_valid, timeout_sec=30, backoff_sec=1)

    def _transfer_leadership_to(
        self,
        admin: Admin,
        rpk: RpkTool,
        topic: str,
        target_id: int,
        current_epoch: int,
    ):
        # retried in the wait loop: the target may briefly reject the
        # transfer, e.g. while catching up right after a restart
        def transferred():
            leader = admin.get_partition_leader(
                namespace="kafka", topic=topic, partition=0
            )
            epoch, _ = self._leader_epoch_and_hwm(rpk, topic)
            if leader == target_id and epoch > current_epoch:
                return True
            try:
                admin.transfer_leadership_to(
                    namespace="kafka",
                    topic=topic,
                    partition=0,
                    target_id=target_id,
                )
            except Exception:
                pass
            return False

        wait_until(transferred, timeout_sec=90, backoff_sec=2)

    def _multi_term_feature_state(self, admin: Admin, node: ClusterNode) -> str:
        features = admin.get_features(node=node)["features"]
        for f in features:
            if f["name"] == "multi_term_segments":
                return f["state"]
        return "absent"

    def _bump_term(self, admin: Admin, rpk: RpkTool, topic: str, current_epoch: int):
        admin.transfer_leadership_to(
            namespace="kafka", topic=topic, partition=0, target_id=None
        )

        def epoch_advanced():
            epoch, _ = self._leader_epoch_and_hwm(rpk, topic)
            return epoch > current_epoch

        wait_until(epoch_advanced, timeout_sec=30, backoff_sec=1)

    def _segment_names(self, topic: str) -> set[str]:
        names: set[str] = set()
        storage = self.redpanda.storage(nodes=self.redpanda.nodes)
        for partition in storage.partitions("kafka", topic):
            segments = cast(
                dict[str, object],
                partition.segments,  # pyright: ignore[reportUnknownMemberType]
            )
            names.update(segments.keys())
        return names

    def _check_epoch_end_offsets(
        self, kcl: KCL, topic: str, epoch_end_offsets: dict[int, int]
    ):
        # retried: right after a restart the partition may not have a
        # leader yet and the query comes back empty
        def check():
            for epoch, end_offset in epoch_end_offsets.items():
                results = kcl.offset_for_leader_epoch(topics=topic, leader_epoch=epoch)
                if (
                    len(results) != 1
                    or results[0].leader_epoch != epoch
                    or results[0].epoch_end_offset != end_offset
                ):
                    self.logger.debug(
                        f"epoch {epoch}: expected end offset {end_offset}, "
                        f"got {results}"
                    )
                    return False
            return True

        wait_until(
            check,
            timeout_sec=60,
            backoff_sec=2,
            err_msg="leader epoch end offsets diverged",
        )

    @cluster(num_nodes=4, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_v1_segments_merge_after_upgrade(self):
        topic = TopicSpec(
            partition_count=1,
            replication_factor=3,
            cleanup_policy=TopicSpec.CLEANUP_COMPACT,
        )
        self.client().create_topic(topic)
        name = topic.name
        admin = Admin(self.redpanda)
        rpk = RpkTool(self.redpanda)
        kcl = KCL(self.redpanda)

        # the compaction scheduler only considers dirty logs, and the
        # pre-upgrade data will be fully compacted by the time the upgrade
        # finishes; a zero dirty ratio keeps compaction running so the
        # post-upgrade stamp rewrite and cross-term merges have rounds to
        # ride along with
        rpk.alter_topic_config(name, "min.cleanable.dirty.ratio", "0")

        # several terms of pre-upgrade data: v1 segments roll on every term
        # change, so each term's data (beginning with its unstamped
        # configuration batch) lives in its own v1 segment
        epoch_end_offsets: dict[int, int] = {}
        for _ in range(3):
            self._produce(name, 200)
            epoch, hwm = self._leader_epoch_and_hwm(rpk, name)
            epoch_end_offsets[epoch] = hwm
            self._bump_term(admin, rpk, name, epoch)

        pre_upgrade_segments = self._segment_names(name)
        assert pre_upgrade_segments and not any(
            n.endswith("-v2") for n in pre_upgrade_segments
        ), f"expected only pre-v2 segments: {pre_upgrade_segments}"

        # upgrade the whole cluster; the multi_term_segments feature
        # activates once every node runs the new version
        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)
        self.redpanda.restart_nodes(self.redpanda.nodes)
        self.redpanda.await_feature("multi_term_segments", "active", timeout_sec=60)

        # a couple more terms of post-upgrade data
        for _ in range(2):
            self._produce(name, 200)
            epoch, hwm = self._leader_epoch_and_hwm(rpk, name)
            epoch_end_offsets[epoch] = hwm
            self._bump_term(admin, rpk, name, epoch)
        self._produce(name, 200)

        # compaction stamps the pre-upgrade configuration batches with their
        # terms, which makes the v1 segments eligible for cross-term
        # adjacent merges; the merge outputs adopt the v2 filename version
        def v1_segments_gone():
            names = self._segment_names(name)
            v1 = sorted(n for n in names if not n.endswith("-v2"))
            self.logger.debug(f"remaining pre-v2 segments: {v1}")
            return len(v1) == 0

        wait_until(v1_segments_gone, timeout_sec=180, backoff_sec=5)

        # leader epoch attribution survived stamping and merging, for both
        # pre- and post-upgrade epochs
        self._check_epoch_end_offsets(kcl, name, epoch_end_offsets)

        # and it survives a full restart, where the merged segments' term
        # spans are recovered from the stamped configuration batches
        self.redpanda.restart_nodes(self.redpanda.nodes)
        self._check_epoch_end_offsets(kcl, name, epoch_end_offsets)

        # strongest form: delete one node's offset indexes so recovery must
        # rebuild every term span from the stamped configuration batches in
        # the log data alone, then answer the epoch queries as that node's
        # leader
        node = self.redpanda.nodes[0]
        node_id = self.redpanda.node_id(node)
        self.redpanda.stop_node(node)
        node.account.ssh(
            f"find {self.redpanda.DATA_DIR}/kafka/{name} -name '*.base_index' -delete"
        )
        self.redpanda.start_node(node)
        epoch, _ = self._leader_epoch_and_hwm(rpk, name)
        self._transfer_leadership_to(admin, rpk, name, node_id, epoch)
        self._check_epoch_end_offsets(kcl, name, epoch_end_offsets)

    @cluster(num_nodes=4, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_mixed_version_cluster_writes_v1_segments(self):
        """
        While the cluster is mixed-version the multi_term_segments feature
        cannot activate, and an upgraded node leading the partition must
        keep writing v1 segments - the remaining old nodes (or the upgraded
        node itself after a rollback) must be able to read everything,
        including the term-stamped configuration batches the upgraded node
        appends.
        """
        topic = TopicSpec(
            partition_count=1,
            replication_factor=3,
            cleanup_policy=TopicSpec.CLEANUP_COMPACT,
        )
        self.client().create_topic(topic)
        name = topic.name
        admin = Admin(self.redpanda)
        rpk = RpkTool(self.redpanda)
        kcl = KCL(self.redpanda)

        epoch_end_offsets: dict[int, int] = {}

        def produce_and_record():
            self._produce(name, 200)
            epoch, hwm = self._leader_epoch_and_hwm(rpk, name)
            epoch_end_offsets[epoch] = hwm
            return epoch

        # a couple of terms of fully-old-cluster data
        for _ in range(2):
            epoch = produce_and_record()
            self._bump_term(admin, rpk, name, epoch)

        # upgrade a single node
        upgraded = self.redpanda.nodes[0]
        upgraded_id = self.redpanda.node_id(upgraded)
        old_node_id = self.redpanda.node_id(self.redpanda.nodes[1])
        self.installer.install([upgraded], RedpandaInstaller.HEAD)
        self.redpanda.restart_nodes([upgraded])

        # mixed version: the feature must not be active, probed on the
        # upgraded node itself (old nodes do not even know the feature)
        assert self._multi_term_feature_state(admin, upgraded) != "active"

        # write terms through the upgraded node, interleaved with an old
        # node, so both binaries produce segments in the mixed cluster
        for target_id in (upgraded_id, old_node_id, upgraded_id):
            epoch, _ = self._leader_epoch_and_hwm(rpk, name)
            self._transfer_leadership_to(admin, rpk, name, target_id, epoch)
            produce_and_record()

        # everything written so far, by either binary, is v1-named
        names = self._segment_names(name)
        assert names and not any(n.endswith("-v2") for n in names), (
            f"expected only pre-v2 segments in a mixed cluster: {names}"
        )

        # roll the upgraded node back: the old binary must parse the log it
        # left behind (v1 segments; configuration batches may carry payload
        # terms, which old readers skip)
        self.installer.install([upgraded], LATEST_RELEASED_MAJOR)
        self.redpanda.restart_nodes([upgraded])

        # leader epoch attribution is intact on the all-old cluster
        self._check_epoch_end_offsets(kcl, name, epoch_end_offsets)
        # and the rolled-back node itself can lead and serve the data
        epoch, _ = self._leader_epoch_and_hwm(rpk, name)
        self._transfer_leadership_to(admin, rpk, name, upgraded_id, epoch)
        self._check_epoch_end_offsets(kcl, name, epoch_end_offsets)
        records = rpk.consume(name, n=50, offset="start", timeout=60)
        assert records, "expected to consume records from the old cluster"
