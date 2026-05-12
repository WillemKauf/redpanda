// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/clustered_property.h"
#include "config/config_not_converged.h"
#include "config/config_store.h"

#include <gtest/gtest.h>

namespace config {
namespace {

class clustered_property_test : public ::testing::Test {
protected:
    config_store store;
    clustered_property<bool> prop{
      store,
      "test_clustered",
      "test description",
      base_property::metadata{},
      /*default=*/false};
};

TEST_F(clustered_property_test, default_state_is_active) {
    EXPECT_TRUE(prop.is_active());
    EXPECT_EQ(prop.local_value(), false);
    EXPECT_EQ(prop.value(), false);
}

TEST_F(clustered_property_test, value_throws_when_not_active) {
    prop.test_set_staged(true);
    EXPECT_FALSE(prop.is_active());
    EXPECT_EQ(prop.local_value(), false);
    EXPECT_THROW(prop.value(), config_not_converged);
}

TEST_F(clustered_property_test, activate_flips_is_active) {
    prop.test_set_staged(true);
    prop.test_activate(true);
    EXPECT_TRUE(prop.is_active());
    prop.test_promote_local(true);
    EXPECT_EQ(prop.local_value(), true);
    EXPECT_EQ(prop.value(), true);
}

TEST_F(clustered_property_test, wait_until_active_resolves_on_activation) {
    ss::abort_source as;
    prop.test_set_staged(true);
    auto fut = prop.wait_until_active(as);
    EXPECT_FALSE(fut.available());
    prop.test_promote_local(true);
    prop.test_activate(true);
    auto v = fut.get();
    EXPECT_EQ(v, true);
}

TEST_F(clustered_property_test, wait_until_active_breaks_on_abort) {
    ss::abort_source as;
    prop.test_set_staged(true);
    auto fut = prop.wait_until_active(as);
    EXPECT_FALSE(fut.available());
    as.request_abort();
    EXPECT_THROW(fut.get(), ss::abort_requested_exception);
}

TEST_F(clustered_property_test, wait_until_active_throws_on_already_aborted) {
    ss::abort_source as;
    as.request_abort();
    prop.test_set_staged(true);
    EXPECT_THROW(
      prop.wait_until_active(as).get(), ss::abort_requested_exception);
}

TEST_F(clustered_property_test, apply_activation_matches_staged_value) {
    prop.test_set_staged(true);
    EXPECT_FALSE(prop.is_active());

    // YAML "true" matches the staged value -> activation succeeds.
    prop.apply_activation("true");
    EXPECT_TRUE(prop.is_active());
    EXPECT_FALSE(prop.staged().has_value());
}

TEST_F(clustered_property_test, apply_activation_mismatch_is_dropped) {
    prop.test_set_staged(true);
    EXPECT_FALSE(prop.is_active());

    // YAML "false" does not match the staged "true" -> activation dropped.
    prop.apply_activation("false");
    EXPECT_FALSE(prop.is_active());
    EXPECT_TRUE(prop.staged().has_value());
}

TEST_F(clustered_property_test, apply_activation_with_no_staged_is_noop) {
    EXPECT_TRUE(prop.is_active());
    prop.apply_activation("true");
    EXPECT_TRUE(prop.is_active());
}

TEST_F(
  clustered_property_test, apply_activation_with_malformed_yaml_is_dropped) {
    prop.test_set_staged(true);
    prop.apply_activation("{not valid yaml: ::");
    EXPECT_FALSE(prop.is_active());
}

TEST_F(clustered_property_test, set_pending_value_stages_for_cluster) {
    auto n = YAML::Load("true");
    prop.set_pending_value(n);
    // Pending slot also set so restart promotion picks up the right value:
    EXPECT_FALSE(prop.is_active());
    EXPECT_TRUE(prop.staged().has_value());
    EXPECT_EQ(*prop.staged(), true);
    EXPECT_EQ(prop.local_value(), false); // not yet promoted locally
}

TEST_F(clustered_property_test, set_pending_value_via_yaml_node_handles_bool) {
    // The exact YAML->bool conversion used by yaml-cpp.
    auto n = YAML::Load("true");
    EXPECT_NO_THROW(prop.set_pending_value(n));
    EXPECT_TRUE(prop.staged().has_value());
}

} // namespace

} // namespace config
