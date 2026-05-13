# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

"""
Integration test for clustered_property<T> convergence via rolling restart.

A clustered_property goes through two distinct phases when its value changes:

  1. Staged: the new value is written to the controller log and every node
     picks it up as a pending (needs_restart) change.  local_value() still
     returns the old value; value() throws config_not_converged.

  2. Active: after ALL live nodes have restarted (promoting pending -> active)
     the controller's activation loop fires a cluster_config_activate_cmd,
     which broadcasts to every shard and sets is_active()=true.  value() no
     longer throws, and callers that relied on the cluster-converged value can
     proceed.

Observable admin-API signals used by this test (no new endpoints needed):

  GET /v1/cluster_config?suppress_pending=true
      Returns the per-node ACTIVE (runtime) value for every property.
      Before restart: returns old value.
      After restart:  returns new value.

  GET /v1/cluster_config/status
      Each node entry has a "restart" bool that is true while a staged
      change has not yet been applied via restart.

The test uses cloud_topics_enabled (defaults to false) as the test subject
because:
  - It is a clustered_property<bool>.
  - Setting it to true has no cross-property validation requirements.
  - The built-in trial license (present in all test environments unless
    explicitly disabled) allows the enterprise property to be set.
"""

from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.tests.redpanda_test import RedpandaTest
from ducktape.utils.util import wait_until


class ClusteredConfigTest(RedpandaTest):
    """
    End-to-end test for clustered_property<T> convergence via rolling restart.
    """

    PROP = "cloud_topics_enabled"

    def __init__(self, test_context):
        super().__init__(
            test_context=test_context,
            num_brokers=3,
            # Disable metadata uploads so background config writes do not
            # interfere with our version-number bookkeeping.
            extra_rp_conf={"enable_cluster_metadata_upload_loop": False},
        )
        self.admin = Admin(self.redpanda)

    def _local_value(self, node) -> bool:
        """
        Read the per-node active (runtime) value of PROP via
        GET /v1/cluster_config?suppress_pending=true.

        suppress_pending=true bypasses the pending slot and returns the value
        that is actually running on this node right now.  For a
        clustered_property this is equivalent to local_value().
        """
        cfg = self.admin.get_cluster_config(node=node, suppress_pending=True)
        return cfg[self.PROP]

    def _all_restart_flags(self) -> dict[int, bool]:
        """
        Return {node_id: restart_needed} for every node in the cluster,
        queried from the controller node.
        """
        status = self.admin.get_cluster_config_status(
            node=self.redpanda.controller()
        )
        return {entry["node_id"]: entry["restart"] for entry in status}

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_rolling_restart_activates_clustered_property(self):
        """
        Verify the full lifecycle of a clustered_property change:

        1. Initial state: cloud_topics_enabled = false on all nodes.
        2. Stage the change: PUT /v1/cluster_config  ->  cloud_topics_enabled=true.
           Every node sees it as "pending" (restart=true).  Local active value
           is still false.
        3. Rolling restart: one node at a time.  After each node restarts its
           local active value flips to true and its restart flag clears.
        4. After all nodes have restarted the controller's activation loop
           detects that every live node has the new value locally and fires a
           cluster_config_activate_cmd.  The property is now ACTIVE cluster-wide.
        """

        # ------------------------------------------------------------------ #
        # Step 1: assert initial state                                        #
        # ------------------------------------------------------------------ #
        for node in self.redpanda.nodes:
            local_val = self._local_value(node)
            assert local_val is False, (
                f"Expected cloud_topics_enabled local_value=false on "
                f"{node.account.hostname} before any change, got {local_val!r}"
            )

        # ------------------------------------------------------------------ #
        # Step 2: stage the change                                            #
        # ------------------------------------------------------------------ #
        self.logger.info("Staging cloud_topics_enabled=true")
        patch_result = self.admin.patch_cluster_config(
            upsert={self.PROP: True}
        )
        new_version = patch_result["config_version"]

        # Wait until every node has acknowledged the new config version.
        wait_until(
            lambda: all(
                entry["config_version"] >= new_version
                for entry in self.admin.get_cluster_config_status(
                    node=self.redpanda.controller()
                )
            ),
            timeout_sec=30,
            backoff_sec=0.5,
            err_msg=(
                f"Not all nodes reached config_version >= {new_version}"
            ),
        )

        # Every node should now report restart=true (change is pending).
        restart_flags = self._all_restart_flags()
        self.logger.info(f"Restart flags after staging: {restart_flags}")
        for node_id, needs_restart in restart_flags.items():
            assert needs_restart, (
                f"Expected node {node_id} to have restart=true after staging "
                f"cloud_topics_enabled=true, but got restart={needs_restart}"
            )

        # Local active value is still false on every node (no restart yet).
        for node in self.redpanda.nodes:
            local_val = self._local_value(node)
            assert local_val is False, (
                f"Expected cloud_topics_enabled local_value=false on "
                f"{node.account.hostname} before rolling restart, "
                f"got {local_val!r}"
            )

        # ------------------------------------------------------------------ #
        # Step 3: rolling restart — one node at a time                       #
        # ------------------------------------------------------------------ #
        for i, node in enumerate(self.redpanda.nodes):
            hostname = node.account.hostname
            self.logger.info(
                f"Rolling restart: restarting node {i+1}/3 ({hostname})"
            )
            self.redpanda.restart_nodes([node])

            # Wait for the cluster to become healthy before moving on.
            wait_until(
                self.redpanda.healthy,
                timeout_sec=60,
                backoff_sec=1,
                err_msg=(
                    f"Cluster not healthy after restarting {hostname}"
                ),
            )

            # The restarted node now has the new local active value.
            local_val = self._local_value(node)
            assert local_val is True, (
                f"Expected cloud_topics_enabled local_value=true on "
                f"{hostname} after restart, got {local_val!r}"
            )

            # The restarted node should have cleared its restart flag.
            node_id = self.redpanda.node_id(node)
            restart_flags = self._all_restart_flags()
            assert not restart_flags.get(node_id, True), (
                f"Expected restart=false for node {node_id} ({hostname}) "
                f"after restart, flags={restart_flags}"
            )

        # ------------------------------------------------------------------ #
        # Step 4: verify full convergence                                     #
        # ------------------------------------------------------------------ #
        self.logger.info(
            "All nodes restarted — verifying cluster-wide convergence"
        )

        # All nodes have the new local active value.
        for node in self.redpanda.nodes:
            local_val = self._local_value(node)
            assert local_val is True, (
                f"Expected cloud_topics_enabled local_value=true on "
                f"{node.account.hostname} after full rolling restart, "
                f"got {local_val!r}"
            )

        # All restart flags have cleared.
        wait_until(
            lambda: not any(self._all_restart_flags().values()),
            timeout_sec=30,
            backoff_sec=0.5,
            err_msg=(
                "Expected all restart flags to be false after full "
                "rolling restart, but some nodes still report restart=true: "
                + str(self._all_restart_flags())
            ),
        )

        # The activation loop fires asynchronously on the controller.  Wait
        # for it: once all nodes have the new local value the controller will
        # replicate a cluster_config_activate_cmd, after which the property
        # is considered cluster-converged and is_active() returns true on
        # every node.  There is no direct admin API for is_active(), but the
        # suppress_pending=false view will show the same value as
        # suppress_pending=true (no more pending-vs-active divergence) and
        # restart=false confirms the promotion happened.
        #
        # Assert both views agree on the new value from every node's
        # perspective as a final sanity check.
        for node in self.redpanda.nodes:
            configured_val = self.admin.get_cluster_config(
                node=node, suppress_pending=False
            )[self.PROP]
            active_val = self._local_value(node)
            assert configured_val is True, (
                f"Expected configured cloud_topics_enabled=true on "
                f"{node.account.hostname}, got {configured_val!r}"
            )
            assert active_val is True, (
                f"Expected active cloud_topics_enabled=true on "
                f"{node.account.hostname}, got {active_val!r}"
            )

        self.logger.info(
            "clustered_property activation via rolling restart: PASS"
        )
