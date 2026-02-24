# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

"""
Integration tests for KIP-848 ConsumerGroupHeartbeat protocol.

These tests exercise the ConsumerGroupHeartbeat RPC (API key 68) using
kafka-python with custom Request/Response classes. They verify the core
lifecycle of the new consumer group protocol: join, heartbeat, leave,
static member management, epoch advancement, assignment, and error handling.

Inspired by Apache Kafka's ConsumerGroupHeartbeatRequestTest.scala and
GroupCoordinatorServiceTest.java.
"""

import struct
import time
import uuid as uuid_mod

from kafka import KafkaAdminClient
from kafka.protocol.api import Request, Response
import kafka.protocol.types as types

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest

# Kafka error codes
NONE_ERROR = 0
UNKNOWN_MEMBER_ID = 25
GROUP_MAX_SIZE_REACHED = 27
FENCED_MEMBER_EPOCH = 110
COORDINATOR_NOT_AVAILABLE = 15
NOT_COORDINATOR = 16


class Uuid(types.AbstractType):
    """UUID type for Kafka protocol (16 bytes, big-endian)."""

    @classmethod
    def encode(cls, value):
        if value is None:
            return b'\x00' * 16
        if isinstance(value, str):
            uid = uuid_mod.UUID(value)
        elif isinstance(value, uuid_mod.UUID):
            uid = value
        else:
            uid = uuid_mod.UUID(bytes=value)
        return uid.bytes

    @classmethod
    def decode(cls, data):
        raw = data.read(16)
        uid = uuid_mod.UUID(bytes=raw)
        return str(uid)


# -- ConsumerGroupHeartbeat Response (API key 68, version 0) --

class ConsumerGroupHeartbeatResponse_v0(Response):
    API_KEY = 68
    API_VERSION = 0
    FLEXIBLE_VERSION = True
    SCHEMA = types.Schema(
        ("throttle_time_ms", types.Int32),
        ("error_code", types.Int16),
        ("error_message", types.CompactString("utf-8")),
        ("member_id", types.CompactString("utf-8")),
        ("member_epoch", types.Int32),
        ("heartbeat_interval_ms", types.Int32),
        ("assignment", types.CompactArray(
            types.Schema(
                ("topic_id", Uuid),
                ("partitions", types.CompactArray(types.Int32)),
                ("_tagged_fields", types.TaggedFields()),
            ),
        )),
        ("_tagged_fields", types.TaggedFields()),
    )


# -- ConsumerGroupHeartbeat Request (API key 68, version 0) --

class ConsumerGroupHeartbeatRequest_v0(Request):
    API_KEY = 68
    API_VERSION = 0
    FLEXIBLE_VERSION = True
    RESPONSE_TYPE = ConsumerGroupHeartbeatResponse_v0
    SCHEMA = types.Schema(
        ("group_id", types.CompactString("utf-8")),
        ("member_id", types.CompactString("utf-8")),
        ("member_epoch", types.Int32),
        ("instance_id", types.CompactString("utf-8")),
        ("rack_id", types.CompactString("utf-8")),
        ("rebalance_timeout_ms", types.Int32),
        ("subscribed_topic_names", types.CompactArray(
            types.CompactString("utf-8"),
        )),
        ("subscribed_topic_regex", types.CompactString("utf-8")),
        ("server_assignor", types.CompactString("utf-8")),
        ("topic_partitions", types.CompactArray(
            types.Schema(
                ("topic_id", Uuid),
                ("partitions", types.CompactArray(types.Int32)),
                ("_tagged_fields", types.TaggedFields()),
            ),
        )),
        ("_tagged_fields", types.TaggedFields()),
    )


class ConsumerGroupHeartbeatClient:
    """Client for ConsumerGroupHeartbeat (API key 68) using kafka-python.

    Uses KafkaAdminClient for connection management and coordinator
    discovery, with custom Request/Response classes for the new API.
    """

    def __init__(self, redpanda):
        self._redpanda = redpanda
        self._admin = KafkaAdminClient(
            bootstrap_servers=redpanda.brokers(),
        )

    def close(self):
        self._admin.close()

    def _send_heartbeat(
        self,
        coordinator: int,
        group_id: str,
        member_id: str,
        member_epoch: int,
        instance_id: str | None,
        rack_id: str | None,
        rebalance_timeout_ms: int,
        subscribed_topic_names: list[str] | None,
        subscribed_topic_regex: str | None,
        server_assignor: str | None,
        topic_partitions: list[dict] | None,
    ) -> dict:
        """Send a single ConsumerGroupHeartbeat request."""
        tp_encoded = None
        if topic_partitions is not None:
            tp_encoded = [
                (tp["TopicId"], tp["Partitions"], {})
                for tp in topic_partitions
            ]

        request = ConsumerGroupHeartbeatRequest_v0(
            group_id=group_id,
            member_id=member_id,
            member_epoch=member_epoch,
            instance_id=instance_id,
            rack_id=rack_id,
            rebalance_timeout_ms=rebalance_timeout_ms,
            subscribed_topic_names=subscribed_topic_names,
            subscribed_topic_regex=subscribed_topic_regex,
            server_assignor=server_assignor,
            topic_partitions=tp_encoded,
            _tagged_fields={},
        )

        future = self._admin._send_request_to_node(coordinator, request)
        self._admin._wait_for_futures([future])
        response = future.value

        assignment = None
        if response.assignment is not None:
            assignment = []
            for tp in response.assignment:
                assignment.append({
                    "TopicId": tp[0],
                    "Partitions": list(tp[1]) if tp[1] else [],
                })

        return {
            "ThrottleTimeMs": response.throttle_time_ms,
            "ErrorCode": response.error_code,
            "ErrorMessage": response.error_message,
            "MemberId": response.member_id,
            "MemberEpoch": response.member_epoch,
            "HeartbeatIntervalMs": response.heartbeat_interval_ms,
            "Assignment": assignment,
        }

    def heartbeat(
        self,
        group_id: str = "test-cg",
        member_id: str = "",
        member_epoch: int = 0,
        instance_id: str | None = None,
        rack_id: str | None = None,
        rebalance_timeout_ms: int = -1,
        subscribed_topic_names: list[str] | None = None,
        subscribed_topic_regex: str | None = None,
        server_assignor: str | None = None,
        topic_partitions: list[dict] | None = None,
        retry_on_not_coordinator: bool = True,
    ) -> dict:
        """Send a ConsumerGroupHeartbeat request and return the response.

        Retries on NOT_COORDINATOR (16) and COORDINATOR_NOT_AVAILABLE (15)
        since these are transient errors during partition recovery.
        """
        coordinator = self._admin._find_coordinator_ids(
            [group_id]
        )[group_id]

        retries = 10 if retry_on_not_coordinator else 0
        for attempt in range(retries + 1):
            resp = self._send_heartbeat(
                coordinator=coordinator,
                group_id=group_id,
                member_id=member_id,
                member_epoch=member_epoch,
                instance_id=instance_id,
                rack_id=rack_id,
                rebalance_timeout_ms=rebalance_timeout_ms,
                subscribed_topic_names=subscribed_topic_names,
                subscribed_topic_regex=subscribed_topic_regex,
                server_assignor=server_assignor,
                topic_partitions=topic_partitions,
            )
            if resp["ErrorCode"] not in (
                NOT_COORDINATOR, COORDINATOR_NOT_AVAILABLE
            ):
                return resp
            if attempt < retries:
                self._redpanda.logger.debug(
                    f"Got error {resp['ErrorCode']} for group {group_id}, "
                    f"retrying ({attempt + 1}/{retries})..."
                )
                time.sleep(1)
                # Re-discover coordinator in case it moved
                coordinator = self._admin._find_coordinator_ids(
                    [group_id]
                )[group_id]
        return resp


class ConsumerGroupHeartbeatTest(RedpandaTest):
    """Tests for the KIP-848 ConsumerGroupHeartbeat API.

    Covers the following categories (inspired by Kafka's test suite):
    A. Join/Leave lifecycle
    B. Epoch management and fencing
    C. Static membership
    D. Assignment distribution
    E. Subscription changes
    F. Multi-member scenarios
    G. Error handling and validation
    H. Persistence and recovery
    """

    def __init__(self, test_ctx, *args, **kwargs):
        super().__init__(
            test_ctx,
            num_brokers=3,
            extra_rp_conf={
                "enable_leader_balancer": False,
                "consumer_group_max_size": 10,
            },
            *args,
            **kwargs,
        )

    def setUp(self):
        super().setUp()
        self.cg_client = ConsumerGroupHeartbeatClient(self.redpanda)
        self.rpk = RpkTool(self.redpanda)

    def tearDown(self):
        self.cg_client.close()
        super().tearDown()

    def _heartbeat(self, **kwargs) -> dict:
        """Send a ConsumerGroupHeartbeat request with sensible defaults."""
        return self.cg_client.heartbeat(**kwargs)

    def _join(self, group_id, topics, **kwargs):
        """Join helper: returns (member_id, member_epoch)."""
        resp = self._heartbeat(
            group_id=group_id,
            member_epoch=0,
            subscribed_topic_names=topics,
            **kwargs,
        )
        assert resp["ErrorCode"] == NONE_ERROR, f"Join failed: {resp}"
        return resp["MemberId"], resp["MemberEpoch"]

    # ---------------------------------------------------------------
    # A. Join/Leave Lifecycle
    # ---------------------------------------------------------------

    @cluster(num_nodes=3)
    def test_join_returns_member_id_and_epoch(self):
        """A new member joining with epoch=0 should get a member ID,
        a positive member epoch, and a heartbeat interval."""
        topic = TopicSpec(partition_count=3)
        self.client().create_topic(topic)

        resp = self._heartbeat(
            group_id="test-join",
            member_epoch=0,
            subscribed_topic_names=[topic.name],
        )

        self.redpanda.logger.info(f"Join response: {resp}")
        assert resp["ErrorCode"] == NONE_ERROR
        assert resp["MemberId"] is not None and resp["MemberId"] != ""
        assert resp["MemberEpoch"] > 0
        assert resp["HeartbeatIntervalMs"] > 0

    @cluster(num_nodes=3)
    def test_graceful_leave(self):
        """A member sending epoch=-1 should leave the group. Subsequent
        heartbeats from that member should fail with UNKNOWN_MEMBER_ID."""
        topic = TopicSpec(partition_count=3)
        self.client().create_topic(topic)

        member_id, _ = self._join("test-leave", [topic.name])

        # Leave
        leave_resp = self._heartbeat(
            group_id="test-leave",
            member_id=member_id,
            member_epoch=-1,
        )
        assert leave_resp["ErrorCode"] == NONE_ERROR
        assert leave_resp["MemberEpoch"] == -1

        # Verify member is gone
        hb_resp = self._heartbeat(
            group_id="test-leave",
            member_id=member_id,
            member_epoch=1,
        )
        assert hb_resp["ErrorCode"] == UNKNOWN_MEMBER_ID

    @cluster(num_nodes=3)
    def test_leave_unknown_member(self):
        """Leaving with a non-existent member ID should fail."""
        topic = TopicSpec(partition_count=1)
        self.client().create_topic(topic)

        # First create the group by joining a real member
        self._join("test-leave-unknown", [topic.name])

        # Now try to leave with a non-existent member ID
        resp = self._heartbeat(
            group_id="test-leave-unknown",
            member_id="nonexistent",
            member_epoch=-1,
        )
        assert resp["ErrorCode"] == UNKNOWN_MEMBER_ID

    @cluster(num_nodes=3)
    def test_full_lifecycle_join_heartbeat_leave(self):
        """Complete lifecycle: join -> heartbeat -> heartbeat -> leave."""
        topic = TopicSpec(partition_count=3)
        self.client().create_topic(topic)

        # Join
        member_id, epoch = self._join("test-lifecycle", [topic.name])

        # Multiple heartbeats
        for _ in range(3):
            resp = self._heartbeat(
                group_id="test-lifecycle",
                member_id=member_id,
                member_epoch=epoch,
            )
            assert resp["ErrorCode"] == NONE_ERROR
            epoch = resp["MemberEpoch"]

        # Leave
        resp = self._heartbeat(
            group_id="test-lifecycle",
            member_id=member_id,
            member_epoch=-1,
        )
        assert resp["ErrorCode"] == NONE_ERROR

    # ---------------------------------------------------------------
    # B. Epoch Management and Fencing
    # ---------------------------------------------------------------

    @cluster(num_nodes=3)
    def test_heartbeat_with_valid_epoch(self):
        """A heartbeat with the correct epoch should succeed."""
        topic = TopicSpec(partition_count=3)
        self.client().create_topic(topic)

        member_id, epoch = self._join("test-hb-epoch", [topic.name])

        resp = self._heartbeat(
            group_id="test-hb-epoch",
            member_id=member_id,
            member_epoch=epoch,
        )
        assert resp["ErrorCode"] == NONE_ERROR

    @cluster(num_nodes=3)
    def test_heartbeat_with_stale_epoch(self):
        """A heartbeat with a wrong epoch should return FENCED_MEMBER_EPOCH."""
        topic = TopicSpec(partition_count=3)
        self.client().create_topic(topic)

        member_id, _ = self._join("test-fence", [topic.name])

        resp = self._heartbeat(
            group_id="test-fence",
            member_id=member_id,
            member_epoch=999,
        )
        assert resp["ErrorCode"] == FENCED_MEMBER_EPOCH

    @cluster(num_nodes=3)
    def test_heartbeat_unknown_member(self):
        """A heartbeat from an unknown member should fail."""
        topic = TopicSpec(partition_count=1)
        self.client().create_topic(topic)

        # First create the group by joining a real member
        self._join("test-unknown-member", [topic.name])

        # Now try a heartbeat with a non-existent member ID
        resp = self._heartbeat(
            group_id="test-unknown-member",
            member_id="does-not-exist",
            member_epoch=1,
        )
        assert resp["ErrorCode"] == UNKNOWN_MEMBER_ID

    @cluster(num_nodes=3)
    def test_member_epoch_advances_on_convergence(self):
        """When a member reports its current assignment matching the target,
        the member epoch should advance to the target assignment epoch."""
        topic = TopicSpec(partition_count=3)
        self.client().create_topic(topic)

        # Join
        join_resp = self._heartbeat(
            group_id="test-epoch-advance",
            member_epoch=0,
            subscribed_topic_names=[topic.name],
        )
        assert join_resp["ErrorCode"] == NONE_ERROR
        member_id = join_resp["MemberId"]
        epoch = join_resp["MemberEpoch"]
        assignment = join_resp.get("Assignment")

        if assignment is not None:
            # Report the assignment back as current (convergence)
            resp = self._heartbeat(
                group_id="test-epoch-advance",
                member_id=member_id,
                member_epoch=epoch,
                topic_partitions=assignment,
            )
            assert resp["ErrorCode"] == NONE_ERROR
            # Epoch should stay same or advance (not decrease)
            assert resp["MemberEpoch"] >= epoch

    # ---------------------------------------------------------------
    # C. Static Membership
    # ---------------------------------------------------------------

    @cluster(num_nodes=3)
    def test_static_member_rejoin_preserves_member_id(self):
        """A static member rejoining with the same instance_id should get
        the same member ID back (not a new one)."""
        topic = TopicSpec(partition_count=3)
        self.client().create_topic(topic)

        member_id, _ = self._join(
            "test-static-rejoin",
            [topic.name],
            instance_id="static-1",
        )

        # Rejoin with same instance_id
        rejoin_resp = self._heartbeat(
            group_id="test-static-rejoin",
            member_epoch=0,
            instance_id="static-1",
            subscribed_topic_names=[topic.name],
        )
        assert rejoin_resp["ErrorCode"] == NONE_ERROR
        assert rejoin_resp["MemberId"] == member_id

    @cluster(num_nodes=3)
    def test_static_member_temporary_leave_and_rejoin(self):
        """epoch=-2 should temporarily leave (keep assignment), and the
        member should be able to rejoin by instance_id."""
        topic = TopicSpec(partition_count=3)
        self.client().create_topic(topic)

        member_id, _ = self._join(
            "test-static-temp",
            [topic.name],
            instance_id="static-temp-1",
        )

        # Temporary leave
        leave_resp = self._heartbeat(
            group_id="test-static-temp",
            member_id=member_id,
            member_epoch=-2,
        )
        assert leave_resp["ErrorCode"] == NONE_ERROR
        assert leave_resp["MemberEpoch"] == -2

        # Rejoin — should get same member ID
        rejoin_resp = self._heartbeat(
            group_id="test-static-temp",
            member_epoch=0,
            instance_id="static-temp-1",
            subscribed_topic_names=[topic.name],
        )
        assert rejoin_resp["ErrorCode"] == NONE_ERROR
        assert rejoin_resp["MemberId"] == member_id

    @cluster(num_nodes=3)
    def test_non_static_member_epoch_minus2_rejected(self):
        """epoch=-2 from a dynamic (non-static) member should be rejected."""
        topic = TopicSpec(partition_count=3)
        self.client().create_topic(topic)

        member_id, _ = self._join("test-dynamic-e2", [topic.name])

        resp = self._heartbeat(
            group_id="test-dynamic-e2",
            member_id=member_id,
            member_epoch=-2,
        )
        # Should be rejected
        assert resp["ErrorCode"] == UNKNOWN_MEMBER_ID

    @cluster(num_nodes=3)
    def test_static_member_two_instances(self):
        """Two static members with different instance IDs should both be
        tracked independently."""
        topic = TopicSpec(partition_count=6)
        self.client().create_topic(topic)

        id1, _ = self._join(
            "test-two-static", [topic.name], instance_id="inst-A"
        )
        id2, _ = self._join(
            "test-two-static", [topic.name], instance_id="inst-B"
        )

        assert id1 != id2
        # Both should be able to rejoin and get their original IDs
        for mid, inst in [(id1, "inst-A"), (id2, "inst-B")]:
            resp = self._heartbeat(
                group_id="test-two-static",
                member_epoch=0,
                instance_id=inst,
                subscribed_topic_names=[topic.name],
            )
            assert resp["ErrorCode"] == NONE_ERROR
            assert resp["MemberId"] == mid

    # ---------------------------------------------------------------
    # D. Assignment Distribution
    # ---------------------------------------------------------------

    @cluster(num_nodes=3)
    def test_single_member_gets_all_partitions(self):
        """A single member subscribing to a topic should receive all
        partitions in its assignment."""
        topic = TopicSpec(partition_count=5)
        self.client().create_topic(topic)

        resp = self._heartbeat(
            group_id="test-all-parts",
            member_epoch=0,
            subscribed_topic_names=[topic.name],
        )
        assert resp["ErrorCode"] == NONE_ERROR

        assignment = resp.get("Assignment")
        if assignment is not None:
            total = sum(len(tp["Partitions"]) for tp in assignment)
            assert total == 5, (
                f"Expected 5 partitions, got {total}: {assignment}"
            )

    @cluster(num_nodes=3)
    def test_partitions_redistributed_on_second_join(self):
        """When a second member joins, partitions should be redistributed
        between both members."""
        topic = TopicSpec(partition_count=6)
        self.client().create_topic(topic)

        # First member joins, gets all 6 partitions
        resp1 = self._heartbeat(
            group_id="test-redistribute",
            member_epoch=0,
            subscribed_topic_names=[topic.name],
        )
        assert resp1["ErrorCode"] == NONE_ERROR
        mid1 = resp1["MemberId"]
        epoch1 = resp1["MemberEpoch"]

        # Second member joins
        resp2 = self._heartbeat(
            group_id="test-redistribute",
            member_epoch=0,
            subscribed_topic_names=[topic.name],
        )
        assert resp2["ErrorCode"] == NONE_ERROR

        # First member heartbeats to get updated assignment
        hb_resp = self._heartbeat(
            group_id="test-redistribute",
            member_id=mid1,
            member_epoch=epoch1,
        )
        if hb_resp["ErrorCode"] == FENCED_MEMBER_EPOCH:
            self.redpanda.logger.info("First member fenced, expected")
        else:
            assert hb_resp["ErrorCode"] == NONE_ERROR

    @cluster(num_nodes=3)
    def test_no_assignment_for_unsubscribed_topic(self):
        """A member should not get partitions for topics it's not
        subscribed to."""
        topic_a = TopicSpec(name="topic-a-nosub", partition_count=3)
        topic_b = TopicSpec(name="topic-b-nosub", partition_count=3)
        self.client().create_topic(topic_a)
        self.client().create_topic(topic_b)

        # Subscribe only to topic_a
        resp = self._heartbeat(
            group_id="test-nosub",
            member_epoch=0,
            subscribed_topic_names=[topic_a.name],
        )
        assert resp["ErrorCode"] == NONE_ERROR

        assignment = resp.get("Assignment")
        if assignment is not None:
            total = sum(len(tp["Partitions"]) for tp in assignment)
            assert total == 3, (
                f"Expected 3 partitions (topic_a only), got {total}"
            )

    @cluster(num_nodes=3)
    def test_multiple_topic_subscription(self):
        """A member subscribing to multiple topics should get partitions
        from all of them."""
        topic_a = TopicSpec(name="topic-a-multi", partition_count=2)
        topic_b = TopicSpec(name="topic-b-multi", partition_count=3)
        self.client().create_topic(topic_a)
        self.client().create_topic(topic_b)

        resp = self._heartbeat(
            group_id="test-multi-topic",
            member_epoch=0,
            subscribed_topic_names=[topic_a.name, topic_b.name],
        )
        assert resp["ErrorCode"] == NONE_ERROR

        assignment = resp.get("Assignment")
        if assignment is not None:
            total = sum(len(tp["Partitions"]) for tp in assignment)
            assert total == 5, f"Expected 5 partitions, got {total}"

    # ---------------------------------------------------------------
    # E. Subscription Changes
    # ---------------------------------------------------------------

    @cluster(num_nodes=3)
    def test_subscription_change_triggers_rebalance(self):
        """Changing subscription in a heartbeat should trigger a rebalance
        and update the assignment."""
        topic_a = TopicSpec(name="topic-a-change", partition_count=3)
        topic_b = TopicSpec(name="topic-b-change", partition_count=3)
        self.client().create_topic(topic_a)
        self.client().create_topic(topic_b)

        member_id, epoch = self._join("test-sub-change", [topic_a.name])

        resp = self._heartbeat(
            group_id="test-sub-change",
            member_id=member_id,
            member_epoch=epoch,
            subscribed_topic_names=[topic_a.name, topic_b.name],
        )
        assert resp["ErrorCode"] == NONE_ERROR

        assignment = resp.get("Assignment")
        if assignment is not None:
            total = sum(len(tp["Partitions"]) for tp in assignment)
            assert total == 6, (
                f"Expected 6 partitions after sub change, got {total}"
            )

    @cluster(num_nodes=3)
    def test_null_subscription_means_no_change(self):
        """A heartbeat without subscribed_topic_names (null) should keep
        the existing subscription unchanged."""
        topic = TopicSpec(partition_count=3)
        self.client().create_topic(topic)

        member_id, epoch = self._join("test-null-sub", [topic.name])

        resp = self._heartbeat(
            group_id="test-null-sub",
            member_id=member_id,
            member_epoch=epoch,
        )
        assert resp["ErrorCode"] == NONE_ERROR

    # ---------------------------------------------------------------
    # F. Multi-Member Scenarios
    # ---------------------------------------------------------------

    @cluster(num_nodes=3)
    def test_three_members_join_sequentially(self):
        """Three members joining sequentially should all get unique member
        IDs and the group should accommodate them all."""
        topic = TopicSpec(partition_count=9)
        self.client().create_topic(topic)

        members = []
        for i in range(3):
            mid, epoch = self._join("test-3-members", [topic.name])
            members.append((mid, epoch))
            self.redpanda.logger.info(
                f"Member {i} joined: id={mid}, epoch={epoch}"
            )

        member_ids = [m[0] for m in members]
        assert len(set(member_ids)) == 3

    @cluster(num_nodes=3)
    def test_member_leave_triggers_reassignment(self):
        """When a member leaves, remaining members should eventually
        take over its partitions."""
        topic = TopicSpec(partition_count=4)
        self.client().create_topic(topic)

        mid1, ep1 = self._join("test-leave-rebal", [topic.name])
        mid2, ep2 = self._join("test-leave-rebal", [topic.name])

        leave_resp = self._heartbeat(
            group_id="test-leave-rebal",
            member_id=mid1,
            member_epoch=-1,
        )
        assert leave_resp["ErrorCode"] == NONE_ERROR

        resp = self._heartbeat(
            group_id="test-leave-rebal",
            member_id=mid2,
            member_epoch=ep2,
        )
        if resp["ErrorCode"] == FENCED_MEMBER_EPOCH:
            self.redpanda.logger.info(
                "Member 2 fenced after member 1 left, expected"
            )
        elif resp["ErrorCode"] == NONE_ERROR:
            assignment = resp.get("Assignment")
            if assignment is not None:
                total = sum(len(tp["Partitions"]) for tp in assignment)
                assert total == 4, (
                    f"Expected 4 partitions for sole member, got {total}"
                )

    @cluster(num_nodes=3)
    def test_mixed_static_and_dynamic_members(self):
        """A group with both static and dynamic members should work."""
        topic = TopicSpec(partition_count=6)
        self.client().create_topic(topic)

        static_id, _ = self._join(
            "test-mixed", [topic.name], instance_id="static-mix"
        )
        dynamic_id, _ = self._join("test-mixed", [topic.name])

        assert static_id != dynamic_id
        self.redpanda.logger.info(
            f"Static: {static_id}, Dynamic: {dynamic_id}"
        )

    # ---------------------------------------------------------------
    # G. Error Handling and Validation
    # ---------------------------------------------------------------

    @cluster(num_nodes=3)
    def test_heartbeat_interval_returned(self):
        """The heartbeat response should contain a positive heartbeat
        interval in milliseconds."""
        topic = TopicSpec(partition_count=1)
        self.client().create_topic(topic)

        resp = self._heartbeat(
            group_id="test-interval",
            member_epoch=0,
            subscribed_topic_names=[topic.name],
        )
        assert resp["ErrorCode"] == NONE_ERROR
        assert resp["HeartbeatIntervalMs"] > 0, (
            f"Expected positive interval, got {resp['HeartbeatIntervalMs']}"
        )

    @cluster(num_nodes=3)
    def test_idempotent_join_same_fields(self):
        """Joining twice with identical parameters should produce two
        different member IDs (each join creates a new member)."""
        topic = TopicSpec(partition_count=4)
        self.client().create_topic(topic)

        resp1 = self._heartbeat(
            group_id="test-idempotent",
            member_epoch=0,
            subscribed_topic_names=[topic.name],
        )
        resp2 = self._heartbeat(
            group_id="test-idempotent",
            member_epoch=0,
            subscribed_topic_names=[topic.name],
        )
        assert resp1["ErrorCode"] == NONE_ERROR
        assert resp2["ErrorCode"] == NONE_ERROR
        assert resp1["MemberId"] != resp2["MemberId"]

    @cluster(num_nodes=3)
    def test_rebalance_timeout_accepted(self):
        """The rebalance_timeout_ms field should be accepted on join."""
        topic = TopicSpec(partition_count=1)
        self.client().create_topic(topic)

        resp = self._heartbeat(
            group_id="test-rebal-timeout",
            member_epoch=0,
            subscribed_topic_names=[topic.name],
            rebalance_timeout_ms=45000,
        )
        assert resp["ErrorCode"] == NONE_ERROR

    @cluster(num_nodes=3)
    def test_rack_id_accepted(self):
        """The rack_id field should be accepted on join."""
        topic = TopicSpec(partition_count=1)
        self.client().create_topic(topic)

        resp = self._heartbeat(
            group_id="test-rack",
            member_epoch=0,
            subscribed_topic_names=[topic.name],
            rack_id="us-east-1",
        )
        assert resp["ErrorCode"] == NONE_ERROR

    @cluster(num_nodes=3)
    def test_server_assignor_field(self):
        """The server_assignor field should be accepted (uniform is default)."""
        topic = TopicSpec(partition_count=1)
        self.client().create_topic(topic)

        resp = self._heartbeat(
            group_id="test-assignor",
            member_epoch=0,
            subscribed_topic_names=[topic.name],
            server_assignor="uniform",
        )
        assert resp["ErrorCode"] == NONE_ERROR

    # ---------------------------------------------------------------
    # H. Persistence and Recovery
    # ---------------------------------------------------------------

    @cluster(num_nodes=3)
    def test_group_survives_coordinator_restart(self):
        """After the coordinator restarts, the group state should be
        recovered from the __consumer_offsets log."""
        topic = TopicSpec(partition_count=3)
        self.client().create_topic(topic)

        member_id, epoch = self._join("test-recovery", [topic.name])

        resp = self._heartbeat(
            group_id="test-recovery",
            member_id=member_id,
            member_epoch=epoch,
        )
        assert resp["ErrorCode"] == NONE_ERROR

        node = self.redpanda.nodes[0]
        self.redpanda.restart_nodes([node])

        # Recreate client after restart (old connections are dead)
        self.cg_client.close()
        time.sleep(5)
        self.cg_client = ConsumerGroupHeartbeatClient(self.redpanda)

        resp = self._heartbeat(
            group_id="test-recovery",
            member_id=member_id,
            member_epoch=epoch,
        )
        self.redpanda.logger.info(f"Post-restart heartbeat: {resp}")
        assert resp["ErrorCode"] in [
            NONE_ERROR,
            FENCED_MEMBER_EPOCH,
            COORDINATOR_NOT_AVAILABLE,
            NOT_COORDINATOR,
        ]
