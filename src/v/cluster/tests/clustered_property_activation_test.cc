// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/types.h"
#include "config/clustered_property.h"
#include "config/config_not_converged.h"
#include "config/config_store.h"
#include "config/property.h"

#include <gtest/gtest.h>

// Tests for the cluster_config_activate_cmd apply path: the STM replicas
// call apply_activation() when the controller leader replicates a
// cluster_config_activate_cmd.  These tests sit in the cluster namespace to
// exercise the interaction with cluster::cluster_config_activate_cmd_data and
// config::collect_clustered_config.

namespace cluster {
namespace {

class clustered_property_activation_test : public ::testing::Test {
protected:
    config::config_store store;
    config::clustered_property<bool> prop{
      store,
      "test_prop",
      "test description",
      config::base_property::metadata{},
      /*default=*/false};
};

// Stage a value and then deliver a matching activate cmd (as the STM does on
// every replica).  Expect the property to flip to active and clear staged.
TEST_F(
  clustered_property_activation_test, activate_cmd_data_round_trip_succeeds) {
    prop.test_set_staged(true);
    EXPECT_FALSE(prop.is_active());
    EXPECT_THROW(prop.value(), config::config_not_converged);
    EXPECT_EQ(prop.staged_yaml_string(), "true");

    cluster_config_activate_cmd_data data{
      .property_name = "test_prop", .value = "true"};
    prop.apply_activation(data.value);

    EXPECT_TRUE(prop.is_active());
    EXPECT_FALSE(prop.staged().has_value());
    prop.test_promote_local(true);
    EXPECT_EQ(prop.local_value(), true);
    EXPECT_EQ(prop.value(), true);
}

// If the activate cmd carries a value that does not match the staged value
// (e.g. a newer delta raced past the activate cmd), the activation must be
// silently dropped so the newer staged value can be activated later.
TEST_F(
  clustered_property_activation_test,
  activate_cmd_data_mismatch_drops_activation) {
    prop.test_set_staged(true);
    EXPECT_FALSE(prop.is_active());

    cluster_config_activate_cmd_data data{
      .property_name = "test_prop", .value = "false"};
    prop.apply_activation(data.value);

    EXPECT_FALSE(prop.is_active());
    EXPECT_TRUE(prop.staged().has_value());
    EXPECT_EQ(*prop.staged(), true);
}

// collect_clustered_config must include the current local value of every
// clustered property so the leader can compare it against the staged value
// for convergence.
TEST_F(
  clustered_property_activation_test,
  collect_clustered_config_captures_local_value) {
    prop.test_promote_local(true);
    prop.set_value_and_activate(true);

    auto snap = config::collect_clustered_config(store);
    auto it = snap.find("test_prop");
    ASSERT_NE(it, snap.end());
    EXPECT_EQ(it->second, "true");
}

// A non-clustered property must always report is_active() == true regardless
// of any staged updates, so it never blocks convergence checks.
TEST_F(
  clustered_property_activation_test, non_clustered_property_is_always_active) {
    config::property<int> p{
      store,
      "non_clustered",
      "desc",
      config::base_property::metadata{},
      /*default=*/0};
    EXPECT_TRUE(p.is_active());
    EXPECT_EQ(p.staged_yaml_string(), "");
}

} // namespace
} // namespace cluster
