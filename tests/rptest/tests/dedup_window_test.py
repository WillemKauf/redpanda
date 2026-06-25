# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time

from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest

DEDUP_WINDOW_PROPERTY = "redpanda.dedup.window.ms"
DEDUP_ID_HEADER = "redpanda.dedup.id"


class DedupWindowTest(RedpandaTest):
    """End-to-end coverage for the per-topic produce-path deduplication window
    (redpanda.dedup.window.ms).

    Deduplication keys off the `redpanda.dedup.id` record header. Records are
    produced one-per-batch with a plain, non-idempotent producer (kcat); for
    single-record batches a duplicate is dropped entirely and never advances
    the partition offset, so the high watermark equals the number of records
    actually stored."""

    def __init__(self, test_context: TestContext) -> None:
        super().__init__(test_context=test_context, num_brokers=1)
        self.rpk = RpkTool(self.redpanda)
        self.kcat = KafkaCat(self.redpanda)

    def _high_watermark(self, topic: str, partition: int = 0) -> int:
        parts = {p.id: p for p in self.rpk.describe_topic(topic, tolerant=True)}
        assert partition in parts, f"partition {partition} not in {parts}"
        return parts[partition].high_watermark

    def _produce_id(self, topic: str, dedup_id: str, n: int = 1):
        for _ in range(n):
            self.kcat.produce_one_with_header(topic, DEDUP_ID_HEADER, dedup_id)

    def _produce_no_id(self, topic: str, n: int = 1):
        for _ in range(n):
            self.kcat.produce_one(topic, "no-id")

    @cluster(num_nodes=1)
    def test_duplicate_ids_are_deduplicated(self):
        topic = "dedup-on"
        self.rpk.create_topic(
            topic,
            partitions=1,
            replicas=1,
            config={DEDUP_WINDOW_PROPERTY: "60000"},
        )

        # Each dedup-id is produced repeatedly within the window; only the
        # first occurrence of each should be stored.
        self._produce_id(topic, "id-1", n=5)
        self._produce_id(topic, "id-2", n=3)
        self._produce_id(topic, "id-3", n=1)
        # Records without the header are never deduplicated.
        self._produce_no_id(topic, n=2)

        # 3 unique ids + 2 header-less records = 5 stored records.
        wait_until(
            lambda: self._high_watermark(topic) == 5,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="expected 5 stored records (3 unique ids + 2 un-tagged), "
            f"got {self._high_watermark(topic)}",
        )

    @cluster(num_nodes=1)
    def test_disabled_by_default_is_a_no_op(self):
        topic = "dedup-off"
        # No dedup property set: the feature must be a complete no-op, even for
        # records that carry the dedup-id header.
        self.rpk.create_topic(topic, partitions=1, replicas=1)

        self._produce_id(topic, "id-1", n=5)

        wait_until(
            lambda: self._high_watermark(topic) == 5,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="dedup must be disabled when the property is unset; "
            f"expected 5 stored records, got {self._high_watermark(topic)}",
        )

    @cluster(num_nodes=1)
    def test_id_readmitted_after_window_expires(self):
        topic = "dedup-expiry"
        window_ms = 2000
        self.rpk.create_topic(
            topic,
            partitions=1,
            replicas=1,
            config={DEDUP_WINDOW_PROPERTY: str(window_ms)},
        )

        # First occurrence is stored.
        self._produce_id(topic, "id-1")
        wait_until(
            lambda: self._high_watermark(topic) == 1,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="first record should be stored",
        )

        # A second occurrence inside the window is dropped.
        self._produce_id(topic, "id-1")
        assert self._high_watermark(topic) == 1, (
            "duplicate within the window must be dropped"
        )

        # Once the window has elapsed the id is admitted again.
        time.sleep(window_ms / 1000 + 5)
        self._produce_id(topic, "id-1")
        wait_until(
            lambda: self._high_watermark(topic) == 2,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="id should be re-admitted after the window expires",
        )
