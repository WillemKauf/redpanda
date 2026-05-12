# Clustered Config Properties Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `clustered_property<T>` config variant that distinguishes node-local from cluster-converged value semantics, with a convergence detector modeled on `feature_manager`. Migrate `cloud_storage_enabled`, `cloud_topics_enabled`, and `iceberg_enabled` to the new type. Enforce the invariant that the on-disk config cache offset is never ahead of the latest controller-STM-snapshot offset.

**Architecture:** A new `clustered_property<T>` subclass of `property<T>` adds `_staged` and `_is_active` state. A new STM command `cluster_config_activate_cmd` flips `_is_active` cluster-wide once all members have reported convergence via an extended `node_health_report.clustered_config` map. A background fiber in `config_manager` runs the convergence check on the leader, mirroring `feature_manager::maybe_update_feature_table`. The cache↔snapshot invariant is enforced by ordering: in-memory apply → force controller snapshot → cache write.

**Tech Stack:** C++23 (Seastar coroutines), Bazel, gtest/gmock (`redpanda_cc_gtest`), ducktape (`tests/rptest/`).

**Spec:** `docs/superpowers/specs/2026-05-12-clustered-config-properties-design.md`

**Phases:**
1. The `clustered_property<T>` type (foundation, no convergence yet).
2. STM command for activation + `_staged`/`_is_active` STM apply paths.
3. Activation loop in `config_manager` (health monitor extension + leader-side convergence detection).
4. Cache↔snapshot ordering invariant.
5. Migrate the three motivating properties.
6. ducktape integration tests.

---

## Phase 1: The `clustered_property<T>` Type

This phase adds the type but no convergence logic. `_is_active` defaults to `true`, `_staged` defaults to `nullopt`. Behavior reduces to ordinary `needs_restart::yes` property semantics. Establishes the API surface and unit-test scaffolding for later phases.

### Task 1: Create the `clustered_property<T>` skeleton

**Files:**
- Create: `src/v/config/clustered_property.h`
- Create: `src/v/config/config_not_converged.h`
- Modify: `src/v/config/BUILD`

- [ ] **Step 1: Write the failing test** for the basic API surface.

Create `src/v/config/tests/clustered_property_test.cc`:

```cpp
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
      base_property::metadata{.needs_restart = needs_restart::yes},
      /*default=*/false};
};

TEST_F(clustered_property_test, default_state_is_active) {
    // With no staged update, the property is trivially active.
    EXPECT_TRUE(prop.is_active());
    EXPECT_EQ(prop.local_value(), false);
    EXPECT_EQ(prop.value(), false);
}

TEST_F(clustered_property_test, value_throws_when_not_active) {
    prop.test_set_staged(true);  // simulate STM-apply path
    EXPECT_FALSE(prop.is_active());
    EXPECT_EQ(prop.local_value(), false);  // still old value
    EXPECT_THROW(prop.value(), config_not_converged);
}

TEST_F(clustered_property_test, activate_flips_is_active) {
    prop.test_set_staged(true);
    prop.test_activate(true);
    EXPECT_TRUE(prop.is_active());
    // Promotion happens on restart; here we just simulate it:
    prop.test_promote_local(true);
    EXPECT_EQ(prop.local_value(), true);
    EXPECT_EQ(prop.value(), true);
}

}  // namespace
}  // namespace config
```

The `test_*` methods are test-only setters we'll add to the implementation.

- [ ] **Step 2: Run the test to verify it fails.**

```
bazel test //src/v/config/tests:clustered_property_test
```

Expected: build failure — `clustered_property.h` doesn't exist.

- [ ] **Step 3: Create `config_not_converged.h`.**

```cpp
#pragma once

#include <stdexcept>

namespace config {

/// Thrown by clustered_property<T>::value() when a pending update has not yet
/// been ratified across all alive members.
class config_not_converged : public std::runtime_error {
public:
    explicit config_not_converged(std::string_view property_name)
      : std::runtime_error(fmt::format(
          "clustered config property '{}' is not yet converged across cluster",
          property_name))
      , _property_name(property_name) {}

    std::string_view property_name() const noexcept { return _property_name; }

private:
    std::string _property_name;
};

}  // namespace config
```

- [ ] **Step 4: Create `clustered_property.h`.**

```cpp
#pragma once

#include "config/config_not_converged.h"
#include "config/property.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>

namespace config {

/// A property whose pending updates require cluster-wide convergence before
/// becoming visible to value()-style readers. local_value() is the per-node
/// active value (no throw). value() throws config_not_converged during the
/// convergence window. Callers that prefer to wait can use wait_until_active().
///
/// Has no implicit operator() — every call site must explicitly choose
/// local_value() or value().
template<typename T>
class clustered_property : public property<T> {
public:
    using value_type = T;

    clustered_property(
      config_store& conf,
      std::string_view name,
      std::string_view desc,
      base_property::metadata meta = {},
      T def = T{},
      typename property<T>::validator validator
      = property<T>::noop_validator,
      std::optional<legacy_default<T>> ld = std::nullopt)
      : property<T>(
          conf,
          name,
          desc,
          base_property::metadata{
            .needs_restart = needs_restart::yes,  // always
            .visibility = meta.visibility,
            .secret = meta.secret,
            .aliases = std::move(meta.aliases)},
          std::move(def),
          std::move(validator),
          std::move(ld)) {}

    /// Node-local active value. Reflects the last value this node was
    /// bootstrapped/restarted with. No throw. Use for bootstrap and
    /// local-only decisions.
    T local_value() const { return property<T>::value(); }

    /// Cluster-converged value. Throws config_not_converged if a pending
    /// update has not been ratified across the cluster.
    T value() const {
        if (!_is_active) {
            throw config_not_converged{property<T>::name()};
        }
        return property<T>::value();
    }

    /// True iff there is no staged update AND the cluster has ratified the
    /// last update via config_activate_cmd.
    bool is_active() const noexcept { return _is_active; }

    /// Optional staged value awaiting convergence. Exposed for the
    /// activation loop and for snapshot serialization.
    const std::optional<T>& staged() const noexcept { return _staged; }

    /// Resolves once is_active() becomes true, then returns value().
    /// Abort-aware so it unblocks during shutdown.
    ss::future<T> wait_until_active(ss::abort_source& as) {
        if (_is_active) {
            co_return property<T>::value();
        }
        auto sub = as.subscribe(
          [this]() noexcept { _activation_cv.broken(); });
        co_await _activation_cv.wait([this] { return _is_active; });
        co_return property<T>::value();
    }

    // STM-apply path: stage the cluster-wide value awaiting convergence.
    // Called by apply_staged(YAML::Node) below and by tests.
    void set_staged_value(T v) {
        _staged = std::move(v);
        _is_active = false;
    }

    // STM-apply path: mark the property as cluster-converged. Called only
    // after apply_activation_yaml() validates the activation cmd's value
    // matches _staged.
    void complete_activation() {
        _staged.reset();
        _is_active = true;
        _activation_cv.broadcast();
    }

    // Drop the staged value (e.g., when a newer update_delta_cmd overrides
    // it before activation completed). is_active remains false until the
    // newer staged value is itself activated.
    void clear_staged() noexcept { _staged.reset(); }

    // Test-only helpers (do not use in production code).
    void test_set_staged(T v) { set_staged_value(std::move(v)); }
    void test_activate(T v) {
        if (_staged && *_staged == v) {
            complete_activation();
        }
    }
    void test_promote_local(T v) { property<T>::set_value(std::move(v)); }
    void set_value_and_activate(T v) {  // for test fixtures
        property<T>::set_value(YAML::Node{v});
        set_staged_value(v);
        complete_activation();
    }

    // Delete the implicit operator() — callers must choose local_value
    // or value explicitly.
    T operator()() const = delete;

private:
    std::optional<T> _staged;
    bool _is_active{true};
    ss::condition_variable _activation_cv;
};

}  // namespace config
```

- [ ] **Step 5: Add the new files + test to `src/v/config/BUILD`.**

In `src/v/config/BUILD`, find the existing `redpanda_cc_library` for the config target. Add `clustered_property.h` and `config_not_converged.h` to the `hdrs` list.

Locate the `tests` subdir's `BUILD` (`src/v/config/tests/BUILD`) and add:

```python
redpanda_cc_gtest(
    name = "clustered_property_test",
    timeout = "short",
    srcs = ["clustered_property_test.cc"],
    deps = [
        "//src/v/config",
        "@googletest//:gtest_main",
    ],
)
```

- [ ] **Step 6: Run the test to verify it passes.**

```
bazel test //src/v/config/tests:clustered_property_test
```

Expected: PASS, all three test cases green.

- [ ] **Step 7: Commit.**

```bash
git add src/v/config/clustered_property.h \
        src/v/config/config_not_converged.h \
        src/v/config/tests/clustered_property_test.cc \
        src/v/config/BUILD \
        src/v/config/tests/BUILD
git commit -m "config: add clustered_property<T> skeleton

Introduces clustered_property<T> with local_value()/value()/is_active()
and wait_until_active(). Throw semantics work; STM-apply paths are
exposed via apply_staged/apply_activation. No convergence logic yet;
defaults to is_active = true so behavior reduces to a regular
restart-required property."
```

### Task 2: Add `wait_until_active` test coverage

**Files:**
- Modify: `src/v/config/tests/clustered_property_test.cc`

- [ ] **Step 1: Write the failing test.**

Append to `clustered_property_test.cc`:

```cpp
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
    EXPECT_THROW(fut.get(), ss::broken_condition_variable);
}
```

- [ ] **Step 2: Run the tests.**

```
bazel test //src/v/config/tests:clustered_property_test
```

Expected: both new tests PASS (the implementation from Task 1 already supports them).

- [ ] **Step 3: Commit.**

```bash
git add src/v/config/tests/clustered_property_test.cc
git commit -m "config: cover wait_until_active in clustered_property tests"
```

---

## Phase 2: STM Command + STM Apply Paths

This phase introduces the new `cluster_config_activate_cmd` STM command and wires `cluster_config_delta_cmd` apply to route clustered properties through `apply_staged()` instead of immediate `set_pending_value`.

### Task 3: Define `cluster_config_activate_cmd`

**Files:**
- Modify: `src/v/cluster/types.h` (struct definition near `cluster_config_delta_cmd_data`)
- Modify: `src/v/cluster/commands.h` (command-key declaration)

- [ ] **Step 1: Find existing command-key constants.**

Run:
```
grep -n "cluster_config_delta_cmd_type\|cluster_config_status_cmd_type" src/v/cluster/commands.h
```

Expected output:
```
src/v/cluster/commands.h:NN: ... cluster_config_delta_cmd_type = N
src/v/cluster/commands.h:NN: ... cluster_config_status_cmd_type = N
```

Note the existing key values. The next available key is the smallest unused number in the controller STM command-key space.

- [ ] **Step 2: Add the new command struct in `src/v/cluster/types.h`.**

Locate `cluster_config_delta_cmd_data` (around line 1507). Add immediately after `cluster_config_status_cmd_data`:

```cpp
struct cluster_config_activate_cmd_data
  : serde::envelope<
      cluster_config_activate_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
    /// Name of the clustered property being activated.
    ss::sstring property_name;

    /// The serialized value that the cluster has converged on. The apply
    /// path on each replica compares this against the property's current
    /// _staged value; mismatch (e.g., a newer delta has overwritten the
    /// staged value in flight) results in the activation being dropped.
    ss::sstring value;

    auto serde_fields() { return std::tie(property_name, value); }

    friend bool operator==(
      const cluster_config_activate_cmd_data&,
      const cluster_config_activate_cmd_data&)
      = default;
};
```

- [ ] **Step 3: Add the command alias in `src/v/cluster/commands.h`.**

Find the existing aliases for `cluster_config_delta_cmd` and `cluster_config_status_cmd`. Add immediately after:

```cpp
using cluster_config_activate_cmd = controller_command<
  cluster_config_activate_cmd_key,  // see Step 4
  cluster_config_activate_cmd_data,
  model::record_batch_type::cluster_config_cmd>;
```

- [ ] **Step 4: Add the key constant in `src/v/cluster/commands.h`.**

First, list every existing command key and find the highest:

```
grep -nE "static constexpr.*cmd_key\s*=" src/v/cluster/commands.h \
  | awk -F'=' '{print $2}' | tr -d ' ;' | sort -n | tail -5
```

Take the highest existing key + 1 (call it `N`). Add:

```cpp
static constexpr int8_t cluster_config_activate_cmd_key = N;
```

Then verify uniqueness:

```
grep -E "cmd_key\s*=\s*N\b" src/v/cluster/commands.h | wc -l
```

Expected: 1 (only the new definition).

- [ ] **Step 5: Build to verify.**

```
bazel build //src/v/cluster:cluster
```

Expected: build success.

- [ ] **Step 6: Commit.**

```bash
git add src/v/cluster/types.h src/v/cluster/commands.h
git commit -m "cluster: define cluster_config_activate_cmd

Adds the STM command used by the controller leader to mark a clustered
config property as cluster-converged. Apply paths land in a follow-up
commit."
```

### Task 4: Wire activation cmd into `config_manager::apply_update` dispatch

**Files:**
- Modify: `src/v/cluster/config_manager.h:35-37` (accepted_commands list)
- Modify: `src/v/cluster/config_manager.cc:1017-1031` (dispatch visit)
- Modify: `src/v/cluster/config_manager.cc` (add apply_activate method)
- Modify: `src/v/cluster/config_manager.h` (declare apply_activate)

- [ ] **Step 1: Extend `accepted_commands`.**

`src/v/cluster/config_manager.h:35-37`:

```cpp
static constexpr auto accepted_commands = make_commands_list<
  cluster_config_delta_cmd,
  cluster_config_status_cmd,
  cluster_config_activate_cmd>{};
```

- [ ] **Step 2: Declare `apply_activate` in the private section of `config_manager.h`.**

```cpp
ss::future<std::error_code> apply_activate(cluster_config_activate_cmd&&);
```

- [ ] **Step 3: Extend the `ss::visit` dispatch.**

`src/v/cluster/config_manager.cc:1017-1031`:

```cpp
co_return co_await ss::visit(
  cmd_var,
  [this](cluster_config_delta_cmd cmd) {
      return apply_delta(std::move(cmd));
  },
  [this](cluster_config_status_cmd cmd) {
      return apply_status(std::move(cmd));
  },
  [this](cluster_config_activate_cmd cmd) {
      return apply_activate(std::move(cmd));
  });
```

- [ ] **Step 4: Implement `apply_activate` in `config_manager.cc`.**

Add near the bottom of the file:

```cpp
ss::future<std::error_code>
config_manager::apply_activate(cluster_config_activate_cmd&& cmd) {
    vlog(
      clusterlog.debug,
      "Applying config_activate_cmd for property={}",
      cmd.value.property_name);

    auto& cfg = config::shard_local_cfg();
    auto* p = cfg.get_if(cmd.value.property_name);
    if (p == nullptr) {
        // Property unknown on this binary version. Treat as no-op; the
        // unknown-property handling on this node has already flagged it
        // via apply_delta.
        co_return errc::success;
    }

    // The activation cmd is only meaningful for clustered properties. We
    // route through a virtual method on base_property to avoid a downcast.
    co_await config::shard_local_cfg_apply_activation_all_shards(
      cmd.value.property_name, cmd.value.value);

    co_return errc::success;
}
```

For now, `shard_local_cfg_apply_activation_all_shards` is a free helper that will be added in the next task; it iterates `config_store` and dispatches to the matching clustered property.

- [ ] **Step 5: Build (will fail until Task 5 lands the helper).**

```
bazel build //src/v/cluster:cluster
```

Expected: link error for `shard_local_cfg_apply_activation_all_shards`. This is OK; commit anyway since the next task lands the symbol.

- [ ] **Step 6: Commit (stack with next task).**

Hold the commit until Task 5 completes.

### Task 5: Add the activation dispatch helper on `config_store`

**Files:**
- Modify: `src/v/config/base_property.h` (add virtual `apply_activation` no-op)
- Modify: `src/v/config/clustered_property.h` (override `apply_activation`)
- Modify: `src/v/config/config_store.h` (add `apply_activation` dispatcher)
- Modify: `src/v/config/configuration.h` (or appropriate place) — add `shard_local_cfg_apply_activation_all_shards`

- [ ] **Step 1: Add a virtual no-op `apply_activation` on `base_property` (`src/v/config/base_property.h`).**

After the `promote_pending` virtual declaration (around line 208):

```cpp
/// Apply a successful activation for clustered properties. Default
/// implementation is a no-op; clustered_property<T> overrides.
virtual void apply_activation(std::string_view /*serialized_value*/) {}
```

- [ ] **Step 2: Override `apply_activation` on `clustered_property<T>`.**

In `src/v/config/clustered_property.h`, inside the class:

```cpp
void apply_activation(std::string_view serialized_value) override {
    if (!_staged.has_value()) {
        return;  // already activated or no staged update
    }
    std::optional<T> incoming;
    try {
        incoming = YAML::Load(std::string{serialized_value}).as<T>();
    } catch (...) {
        return;  // malformed value; drop activation
    }
    if (!incoming || *incoming != *_staged) {
        // A newer update_delta_cmd has replaced _staged. Drop this
        // activation; the newer staged value gets its own cycle.
        return;
    }
    complete_activation();
}
```

Note: `complete_activation()` was added on `clustered_property` in Task 1 Step 4. It is the no-arg internal method that flips `_is_active = true`, clears `_staged`, and broadcasts the `_activation_cv`.

- [ ] **Step 3: Add the cross-shard dispatcher.**

In `src/v/config/configuration.h` (or a new helper file), declare:

```cpp
ss::future<> shard_local_cfg_apply_activation_all_shards(
  std::string_view property_name, std::string_view value);
```

Implementation (in `configuration.cc`):

```cpp
ss::future<> shard_local_cfg_apply_activation_all_shards(
  std::string_view property_name, std::string_view value) {
    return ss::smp::invoke_on_all([property_name, value]() {
        auto& cfg = config::shard_local_cfg();
        auto* p = cfg.get_if(property_name);
        if (p != nullptr) {
            p->apply_activation(value);
        }
    });
}
```

- [ ] **Step 4: Write a focused gtest.**

`src/v/cluster/tests/config_manager_activation_test.cc` (or extend an existing config_manager test):

```cpp
TEST_F(config_manager_test, applies_activation_to_clustered_property) {
    // Set up a clustered property in config_store
    cluster_config_delta_cmd_data delta;
    delta.upsert.push_back({.key = "test_clustered_prop", .value = "true"});
    co_await mgr.apply_delta(make_delta_cmd(std::move(delta)));

    EXPECT_FALSE(get_clustered_prop().is_active());
    EXPECT_TRUE(get_clustered_prop().staged().has_value());

    cluster_config_activate_cmd_data activate{
      .property_name = "test_clustered_prop", .value = "true"};
    co_await mgr.apply_update(make_activate_record_batch(std::move(activate)));

    EXPECT_TRUE(get_clustered_prop().is_active());
    EXPECT_FALSE(get_clustered_prop().staged().has_value());
}
```

(The exact test fixture must mirror existing `config_manager_test` patterns — `grep -rn "config_manager_test\|config_manager_fixture" src/v/cluster/tests/` to find them.)

- [ ] **Step 5: Build and run.**

```
bazel build //src/v/cluster:cluster
bazel test //src/v/cluster/tests:config_manager_activation_test
```

Expected: PASS.

- [ ] **Step 6: Commit (combined with Task 4).**

```bash
git add src/v/cluster/config_manager.h \
        src/v/cluster/config_manager.cc \
        src/v/config/base_property.h \
        src/v/config/clustered_property.h \
        src/v/config/configuration.h \
        src/v/config/configuration.cc \
        src/v/cluster/tests/config_manager_activation_test.cc \
        src/v/cluster/tests/BUILD
git commit -m "cluster: apply cluster_config_activate_cmd

config_manager routes the new activation cmd through a virtual on
base_property; clustered_property<T> overrides to validate the
incoming value matches _staged before flipping _is_active. Tests
cover the happy path; convergence loop comes in a follow-up."
```

### Task 6: Route `cluster_config_delta_cmd` through `apply_staged` for clustered properties

**Files:**
- Modify: `src/v/cluster/config_manager.cc:935-997` (apply_delta)
- Modify: `src/v/config/base_property.h` (add virtual `apply_staged`)
- Modify: `src/v/config/clustered_property.h` (override `apply_staged`)
- Modify: `src/v/config/property.h` (default `apply_staged` to `set_pending_value`)

Currently `apply_delta` calls `set_pending_value` for restart-required properties. For clustered properties, we want to *also* set `_staged`. We add a virtual hook so the dispatch is property-type-driven.

- [ ] **Step 1: Add virtual `apply_staged` on `base_property`.**

`src/v/config/base_property.h`, near `set_pending_value`:

```cpp
/// For clustered properties: stage the incoming value for cluster
/// activation. For ordinary needs_restart::yes properties: forwards to
/// set_pending_value. For non-restart-required properties: forwards to
/// set_value.
virtual void apply_staged(YAML::Node n) {
    if (needs_restart() == needs_restart::yes) {
        set_pending_value(std::move(n));
    } else {
        set_value(std::move(n));
    }
}
```

- [ ] **Step 2: Override on `clustered_property<T>`.**

In `clustered_property.h`:

```cpp
void apply_staged(YAML::Node n) override {
    auto v = n.as<T>();
    // Existing needs_restart::yes pending slot stays in sync so restart
    // promotion picks up the right value:
    property<T>::set_pending_value(std::move(n));
    // Clustered-specific: stash _staged + flip _is_active = false.
    set_staged_value(std::move(v));
}
```

`set_staged_value(T)` was added on `clustered_property` in Task 1 Step 4 — internal helper that the YAML-typed virtual delegates to.

- [ ] **Step 3: Update `config_manager::apply_delta` to call `apply_staged`.**

In `config_manager.cc:935-997`, find where the code currently calls `set_pending_value` or `set_value` on each property. Replace those branches with a single call:

```cpp
property->apply_staged(parsed_yaml_node);
```

(The exact line depends on the existing code shape; you may need to consolidate the existing if-else into the virtual call.)

- [ ] **Step 4: Write a test verifying clustered-property staging.**

In `clustered_property_test.cc`:

```cpp
TEST_F(clustered_property_test, apply_staged_yaml_routes_correctly) {
    auto n = YAML::Load("true");
    prop.apply_staged(n);
    EXPECT_FALSE(prop.is_active());
    EXPECT_TRUE(prop.staged().has_value());
    EXPECT_EQ(*prop.staged(), true);
    // Pending slot is also set for restart promotion:
    EXPECT_TRUE(prop.has_pending_value());
    EXPECT_EQ(prop.local_value(), false);  // not yet promoted
}
```

- [ ] **Step 5: Build and run.**

```
bazel build //src/v/cluster:cluster
bazel test //src/v/config/tests:clustered_property_test
bazel test //src/v/cluster/tests:config_manager_activation_test
```

Expected: PASS.

- [ ] **Step 6: Commit.**

```bash
git add src/v/cluster/config_manager.cc \
        src/v/config/base_property.h \
        src/v/config/clustered_property.h \
        src/v/config/tests/clustered_property_test.cc
git commit -m "config: route delta apply through apply_staged virtual

Clustered properties stage the incoming value in _staged (cluster-side)
in addition to the existing pending slot (per-node restart promotion).
Ordinary properties keep their existing behavior via the default virtual."
```

---

## Phase 3: Activation Loop in `config_manager`

This phase adds the leader-side convergence detector. It mirrors `feature_manager::maybe_update_feature_table` closely.

### Task 7: Extend `node_health_report_serde` with `clustered_config`

**Files:**
- Modify: `src/v/cluster/health_monitor_types.h:242-325`

- [ ] **Step 1: Bump version and add field on the serde struct.**

`src/v/cluster/health_monitor_types.h` — locate `node_health_report_serde` (lines 275–325). Bump `serde::version<1>` to `serde::version<2>`. Add a new field:

```cpp
struct node_health_report_serde
  : serde::envelope<
      node_health_report_serde,
      serde::version<2>,
      serde::compat_version<0>> {
    model::node_id id;
    node::local_state local_state;
    topics_t topics;
    std::optional<cluster::drain_status> drain_status;
    node_liveness_report node_liveness_report;

    /// Each entry: {clustered_property_name, serialized local_active value}.
    /// Used by the controller leader to detect when all members have
    /// converged on a clustered property's staged value.
    absl::flat_hash_map<ss::sstring, ss::sstring> clustered_config;

    auto serde_fields() {
        return std::tie(
          id,
          local_state,
          topics,
          drain_status,
          node_liveness_report,
          clustered_config);
    }
};
```

- [ ] **Step 2: Update the non-serde `node_health_report` struct (lines 242-265) to carry the same field.**

```cpp
struct node_health_report {
    model::node_id id;
    node::local_state local_state;
    topics_t topics;
    std::optional<cluster::drain_status> drain_status;
    node_liveness_report node_liveness_report;
    absl::flat_hash_map<ss::sstring, ss::sstring> clustered_config;
};
```

- [ ] **Step 3: Update the (de)serialization conversion methods.**

In `health_monitor_types.cc` (or wherever the `to_serde`/`from_serde` functions live), add the `clustered_config` field to both directions. Run:

```
grep -n "node_health_report_serde" src/v/cluster/health_monitor_types.cc
```

to find the conversions; add the field to each.

- [ ] **Step 4: Build.**

```
bazel build //src/v/cluster:cluster
```

Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add src/v/cluster/health_monitor_types.h \
        src/v/cluster/health_monitor_types.cc
git commit -m "cluster: add clustered_config to node_health_report

Carries each clustered property's local_active value to the controller
leader so it can detect when all members have converged on a staged
value. Bumps node_health_report_serde version to 2; compat_version
remains 0."
```

### Task 8: Populate `clustered_config` in the health-report sender

**Files:**
- Modify: `src/v/cluster/health_monitor_backend.cc` (or wherever the local node's health report is composed — find via `grep -n "build_node_report\|collect_node_report\|node_liveness_report.*=" src/v/cluster/health_monitor_backend.cc`)

- [ ] **Step 1: Find the report-composition function.**

```
grep -n "node_health_report\s*{" src/v/cluster/health_monitor_backend.cc
```

The site that constructs a fresh `node_health_report` for the local node is the one to extend.

- [ ] **Step 2: Add a `config_store` walker that emits the clustered-property map.**

In `src/v/config/clustered_property.h` (or a helper file), add:

```cpp
namespace config {

/// Emit a map of {property_name, serialized local_active} for every
/// clustered property registered in the config_store. Used by the health
/// report sender.
absl::flat_hash_map<ss::sstring, ss::sstring>
collect_clustered_config(const config_store& store);

}  // namespace config
```

Implementation in `clustered_property.cc` (new file):

```cpp
#include "config/clustered_property.h"
#include "config/config_store.h"

namespace config {

absl::flat_hash_map<ss::sstring, ss::sstring>
collect_clustered_config(const config_store& store) {
    absl::flat_hash_map<ss::sstring, ss::sstring> out;
    store.for_each([&](const base_property& p) {
        if (p.is_clustered()) {
            // Property emits its current local_active as a YAML scalar.
            out.emplace(ss::sstring{p.name()}, p.to_yaml_string_local());
        }
    });
    return out;
}

}  // namespace config
```

The helpers `is_clustered()` and `to_yaml_string_local()` are virtuals on `base_property` (default false / empty), overridden by `clustered_property<T>`.

- [ ] **Step 3: Add the two new virtuals on `base_property` and override in `clustered_property`.**

`src/v/config/base_property.h`:

```cpp
virtual bool is_clustered() const noexcept { return false; }
virtual ss::sstring to_yaml_string_local() const { return {}; }
```

`src/v/config/clustered_property.h`:

```cpp
bool is_clustered() const noexcept override { return true; }
ss::sstring to_yaml_string_local() const override {
    return YAML::Dump(YAML::Node{property<T>::value()});
}
```

- [ ] **Step 4: Wire `collect_clustered_config` into the health-report sender.**

In `health_monitor_backend.cc` at the report-composition site:

```cpp
node_health_report report{
    .id = self,
    .local_state = build_local_state(),
    .topics = collect_topics(),
    .drain_status = drain_status,
    .node_liveness_report = build_liveness_report(),
    .clustered_config = config::collect_clustered_config(
        config::shard_local_cfg()),
};
```

- [ ] **Step 5: Build.**

```
bazel build //src/v/cluster:cluster
```

Expected: PASS.

- [ ] **Step 6: Commit.**

```bash
git add src/v/config/clustered_property.h \
        src/v/config/clustered_property.cc \
        src/v/config/base_property.h \
        src/v/config/BUILD \
        src/v/cluster/health_monitor_backend.cc
git commit -m "cluster: populate clustered_config in node_health_report

Each node's health report now carries the local_active value of every
registered clustered property. Adds is_clustered() and
to_yaml_string_local() virtuals on base_property."
```

### Task 9: Add health-monitor callback + leader change handling to `config_manager`

**Files:**
- Modify: `src/v/cluster/config_manager.h` (constructor args, members, declarations)
- Modify: `src/v/cluster/config_manager.cc` (constructor body, start, stop)

`config_manager` currently doesn't hold the health monitor. We add it.

- [ ] **Step 1: Add constructor args.**

`config_manager.h:51-58`:

```cpp
config_manager(
  preload_result preload,
  ss::sharded<config_frontend>&,
  ss::sharded<rpc::connection_cache>&,
  ss::sharded<partition_leaders_table>&,
  ss::sharded<cluster::members_table>&,
  ss::sharded<ss::abort_source>&,
  ss::sharded<cluster_recovery_table>&,
  ss::sharded<health_monitor_backend>& hm_backend,
  ss::sharded<health_monitor_frontend>& hm_frontend,
  ss::sharded<controller_stm>& controller_stm);
```

Add members:

```cpp
ss::sharded<health_monitor_backend>& _hm_backend;
ss::sharded<health_monitor_frontend>& _hm_frontend;
ss::sharded<controller_stm>& _controller_stm;
notification_id_type _health_notify_handle{};
bool _am_controller_leader{false};
ss::condition_variable _activation_wait;

// {node_id -> {property_name -> serialized local_active}}
absl::flat_hash_map<
  model::node_id,
  absl::flat_hash_map<ss::sstring, ss::sstring>>
  _node_clustered_values;
```

- [ ] **Step 2: Update the constructor body in `config_manager.cc`.**

Pass through the new args; store the references.

- [ ] **Step 3: Update the call site that constructs `config_manager`.**

```
grep -rn "config_manager(" src/v/cluster/ | grep -v config_manager.h | grep -v config_manager.cc
```

The instantiation lives in `controller.cc` (around line 177 per the original grep). Update the constructor call to pass `_hm_backend`, `_hm_frontend`, `_controller_stm`.

- [ ] **Step 4: Register health-monitor callback in `start()`.**

In `config_manager::start()` (around line 220+ of config_manager.cc), add:

```cpp
_health_notify_handle = _hm_backend.local().register_node_callback(
  [this](
    const node_health_report& report,
    std::optional<ss::lw_shared_ptr<const node_health_report>>) {
      auto& entry = _node_clustered_values[report.id];
      bool changed = (entry != report.clustered_config);
      if (changed) {
          entry = report.clustered_config;
          _activation_wait.signal();
      }
  });
```

- [ ] **Step 5: Reuse existing raft0 leader notification.**

`config_manager` already has `_raft0_leader_changed_notification`. Find its handler and extend:

```cpp
// Inside the handler:
_am_controller_leader = (leader_id == _self);
_node_clustered_values.clear();
if (_am_controller_leader) {
    // Pre-populate own entry for fresh-cluster bootstrap (mirrors
    // feature_manager.cc:168-170).
    _node_clustered_values[_self] = config::collect_clustered_config(
      config::shard_local_cfg());
    _activation_wait.signal();
}
```

- [ ] **Step 6: Unregister in `stop()`.**

```cpp
_hm_backend.local().unregister_node_callback(_health_notify_handle);
_activation_wait.broken();
```

- [ ] **Step 7: Build.**

```
bazel build //src/v/cluster:cluster
```

Expected: PASS.

- [ ] **Step 8: Commit.**

```bash
git add src/v/cluster/config_manager.h \
        src/v/cluster/config_manager.cc \
        src/v/cluster/controller.cc
git commit -m "cluster: wire health-monitor callback into config_manager

config_manager now holds references to the health monitor and tracks
each node's reported clustered_config map. Leader-change clears the
map and pre-populates the leader's own entry to avoid bootstrap
deadlock."
```

### Task 10: Implement the activation loop fiber

**Files:**
- Modify: `src/v/cluster/config_manager.h` (declare loop method)
- Modify: `src/v/cluster/config_manager.cc` (implement loop, replicate)

- [ ] **Step 1: Declare the loop method.**

`config_manager.h`:

```cpp
private:
    ss::future<> maybe_activate_clustered_properties();
    ss::future<> replicate_activate_cmd(
      ss::sstring property_name, ss::sstring serialized_value);
```

- [ ] **Step 2: Spawn the fiber in `start()`.**

```cpp
ssx::background = ssx::spawn_with_gate_then(_gate, [this] {
    return ss::do_until(
      [this] { return _as.local().abort_requested(); },
      [this] { return maybe_activate_clustered_properties(); });
}).handle_exception([](const std::exception_ptr& e) {
    vlog(clusterlog.warn, "Config activation loop exception: {}", e);
});
```

- [ ] **Step 3: Implement `maybe_activate_clustered_properties`.**

```cpp
ss::future<> config_manager::maybe_activate_clustered_properties() {
    // Wait until signalled (new health report or leadership change).
    try {
        co_await _activation_wait.wait();
    } catch (const ss::broken_condition_variable&) {
        co_return;
    }
    if (!_am_controller_leader) {
        co_return;
    }

    auto& cfg = config::shard_local_cfg();
    std::vector<std::pair<ss::sstring, ss::sstring>> to_activate;

    cfg.for_each([&, this](const base_property& p) {
        if (!p.is_clustered()) {
            return;
        }
        if (p.is_active()) {
            return;  // already converged
        }
        auto staged_str = p.staged_yaml_string();
        if (staged_str.empty()) {
            return;  // no pending staged value
        }

        // For each member, require: known entry + alive + matching value.
        for (const auto& node_id : _members.local().node_ids()) {
            auto it = _node_clustered_values.find(node_id);
            if (it == _node_clustered_values.end()) {
                vlog(
                  clusterlog.debug,
                  "Defer activate {}: node {} state unknown",
                  p.name(),
                  node_id);
                return;
            }
            auto is_alive_opt = _hm_frontend.local().is_alive(node_id);
            if (!is_alive_opt.has_value() || *is_alive_opt == alive::no) {
                throw std::runtime_error(fmt::format(
                  "Can't activate {} because node {} is not alive",
                  p.name(),
                  node_id));
            }
            auto v_it = it->second.find(ss::sstring{p.name()});
            if (v_it == it->second.end() || v_it->second != staged_str) {
                vlog(
                  clusterlog.debug,
                  "Defer activate {}: node {} value mismatch",
                  p.name(),
                  node_id);
                return;
            }
        }

        to_activate.emplace_back(ss::sstring{p.name()}, staged_str);
    });

    for (auto& [name, value] : to_activate) {
        co_await replicate_activate_cmd(std::move(name), std::move(value));
    }
}
```

- [ ] **Step 4: Add `is_active()` and `staged_yaml_string()` virtuals on `base_property`.**

`src/v/config/base_property.h`:

```cpp
virtual bool is_active() const noexcept { return true; }
virtual ss::sstring staged_yaml_string() const { return {}; }
```

`src/v/config/clustered_property.h`:

```cpp
bool is_active() const noexcept override { return _is_active; }
ss::sstring staged_yaml_string() const override {
    if (!_staged) {
        return {};
    }
    return YAML::Dump(YAML::Node{*_staged});
}
```

- [ ] **Step 5: Implement `replicate_activate_cmd`.**

```cpp
ss::future<> config_manager::replicate_activate_cmd(
  ss::sstring property_name, ss::sstring serialized_value) {
    cluster_config_activate_cmd_data data{
      .property_name = std::move(property_name),
      .value = std::move(serialized_value)};
    cluster_config_activate_cmd cmd{0, std::move(data)};

    auto timeout = model::timeout_clock::now() + std::chrono::seconds(5);
    auto ec = co_await replicate_and_wait(
      _controller_stm, _as, std::move(cmd), timeout);
    if (ec) {
        vlog(
          clusterlog.warn,
          "Failed to replicate config_activate_cmd: {}",
          ec.message());
    }
}
```

- [ ] **Step 6: Build.**

```
bazel build //src/v/cluster:cluster
```

Expected: PASS.

- [ ] **Step 7: Commit.**

```bash
git add src/v/cluster/config_manager.h \
        src/v/cluster/config_manager.cc \
        src/v/config/base_property.h \
        src/v/config/clustered_property.h
git commit -m "cluster: activation loop for clustered config properties

Mirrors feature_manager::maybe_update_feature_table: iterate every
member, require known state + alive + matching local_active. On
convergence, replicate cluster_config_activate_cmd. Dead/partitioned
nodes block activation (throw + retry); unknown-state nodes defer."
```

### Task 11: Integration test for the activation loop

**Files:**
- Create: `src/v/cluster/tests/clustered_property_activation_test.cc`
- Modify: `src/v/cluster/tests/BUILD`

- [ ] **Step 1: Write the test.**

Locate the existing `config_manager` test fixture (`grep -l "config_manager" src/v/cluster/tests/`). Mirror its setup. Write a test that:

1. Constructs a `config_manager` with a fake `members_table` of 3 nodes.
2. Becomes leader.
3. Applies a `cluster_config_delta_cmd` to stage a clustered property.
4. Simulates 2 of 3 nodes reporting the new value via the health-monitor callback.
5. Verifies no `config_activate_cmd` is replicated yet.
6. Simulates the third node reporting.
7. Verifies a `config_activate_cmd` is replicated and `is_active()` flips to true.

```cpp
TEST_F(clustered_property_activation_fixture, replicates_on_all_converged) {
    // Stage a clustered property.
    co_await stage_clustered_property("test_prop", "true");
    EXPECT_FALSE(get_property().is_active());

    // 2 of 3 nodes report convergence.
    deliver_health_report(node_1, {{"test_prop", "true"}});
    deliver_health_report(node_2, {{"test_prop", "true"}});
    co_await yield_until_quiet();
    EXPECT_FALSE(get_property().is_active());
    EXPECT_EQ(replicated_activate_count(), 0);

    // Third node reports.
    deliver_health_report(node_3, {{"test_prop", "true"}});
    co_await yield_until_quiet();
    EXPECT_TRUE(get_property().is_active());
    EXPECT_EQ(replicated_activate_count(), 1);
}

TEST_F(clustered_property_activation_fixture, defers_on_unknown_node) {
    co_await stage_clustered_property("test_prop", "true");
    // Only one of three nodes reports; activation should not fire.
    deliver_health_report(node_1, {{"test_prop", "true"}});
    co_await yield_until_quiet();
    EXPECT_FALSE(get_property().is_active());
}

TEST_F(clustered_property_activation_fixture, throws_on_dead_node) {
    co_await stage_clustered_property("test_prop", "true");
    set_node_alive(node_3, false);
    deliver_health_report(node_1, {{"test_prop", "true"}});
    deliver_health_report(node_2, {{"test_prop", "true"}});
    co_await yield_until_quiet();
    EXPECT_FALSE(get_property().is_active());
    // The exception thrown inside the loop is caught by the fiber's
    // handle_exception; we verify no activation occurred.
}

TEST_F(clustered_property_activation_fixture, clears_state_on_leader_change) {
    co_await stage_clustered_property("test_prop", "true");
    deliver_health_report(node_1, {{"test_prop", "true"}});
    simulate_leadership_change(/*am_leader=*/false);
    co_await yield_until_quiet();
    EXPECT_EQ(internal_node_value_count(), 0);
}
```

- [ ] **Step 2: Add the test target to BUILD.**

```python
redpanda_cc_gtest(
    name = "clustered_property_activation_test",
    timeout = "moderate",
    srcs = ["clustered_property_activation_test.cc"],
    deps = [
        "//src/v/cluster",
        "//src/v/cluster/tests:cluster_test_fixture",
        "@googletest//:gtest_main",
    ],
)
```

- [ ] **Step 3: Run.**

```
bazel test //src/v/cluster/tests:clustered_property_activation_test
```

Expected: PASS.

- [ ] **Step 4: Commit.**

```bash
git add src/v/cluster/tests/clustered_property_activation_test.cc \
        src/v/cluster/tests/BUILD
git commit -m "cluster: integration tests for activation loop

Covers: all-converged happy path, defer-on-unknown-node, dead-node
backoff, and leader-change state reset."
```

---

## Phase 4: Cache↔Snapshot Ordering Invariant

This phase adds the on-demand controller-snapshot trigger and rewires `config_manager::apply_delta` to force a snapshot before writing the cache.

### Task 12: Add on-demand snapshot API to `controller_stm`

**Files:**
- Modify: `src/v/cluster/controller_stm.h:95-113`
- Modify: `src/v/cluster/controller_stm.cc:109-150`

- [ ] **Step 1: Add public method declaration.**

`controller_stm.h`:

```cpp
/// Force an immediate controller-STM snapshot, bypassing the periodic
/// timer. Used by config_manager to enforce the cache<=snapshot offset
/// invariant after every config delta.
ss::future<> force_snapshot();
```

- [ ] **Step 2: Implement.**

`controller_stm.cc`:

```cpp
ss::future<> controller_stm::force_snapshot() {
    // maybe_make_snapshot already gates on feature flag and does the work.
    // We just bypass the timer and call directly.
    co_await maybe_make_snapshot();
}
```

- [ ] **Step 3: Build.**

```
bazel build //src/v/cluster:cluster
```

Expected: PASS.

- [ ] **Step 4: Commit.**

```bash
git add src/v/cluster/controller_stm.h \
        src/v/cluster/controller_stm.cc
git commit -m "cluster: expose force_snapshot on controller_stm

On-demand snapshot trigger used by config_manager to enforce the
cache<=snapshot offset invariant for config deltas. Bypasses the
periodic timer; reuses maybe_make_snapshot."
```

### Task 13: Force snapshot before cache write in `apply_delta`

**Files:**
- Modify: `src/v/cluster/config_manager.cc:935-997`

- [ ] **Step 1: Read the current `apply_delta` body.**

```
grep -n "apply_delta\|store_delta" src/v/cluster/config_manager.cc | head -20
```

Identify the call to `store_delta` and the in-memory apply step.

- [ ] **Step 2: Insert the force_snapshot call between them.**

In `apply_delta`, after the in-memory apply loop completes and before `store_delta`:

```cpp
// Cache<=snapshot invariant: snapshot must capture this delta before the
// cache is written. If we crash between snapshot and cache write, recovery
// preloads stale cache and STM replay catches up. If we crash after cache
// write, both are current. Never: cache ahead of snapshot.
co_await _controller_stm.local().force_snapshot();

co_await store_delta(data);
```

- [ ] **Step 3: Build.**

```
bazel build //src/v/cluster:cluster
```

Expected: PASS.

- [ ] **Step 4: Commit.**

```bash
git add src/v/cluster/config_manager.cc
git commit -m "cluster: force controller snapshot before cache write

Enforces the invariant that the on-disk config cache offset is never
ahead of the latest controller-STM-snapshot offset. Ordering:
in-memory apply -> force snapshot -> cache write. Crash analysis in
the design doc."
```

### Task 14: Crash test for cache↔snapshot ordering

**Files:**
- Create: `src/v/cluster/tests/config_cache_snapshot_order_test.cc`
- Modify: `src/v/cluster/tests/BUILD`

- [ ] **Step 1: Write a test that simulates a crash between snapshot and cache write.**

The test:
1. Configures a `controller_stm` and `config_manager` with a fake snapshot sink that records when `force_snapshot` is called.
2. Patches `store_delta` to throw after the snapshot completes.
3. Applies a config delta.
4. Verifies the snapshot recorded the new config value.
5. Verifies the on-disk cache does NOT contain the new value (because store_delta threw).
6. Reinitializes `config_manager` from the snapshot.
7. Verifies the in-memory value matches the snapshot.

```cpp
TEST_F(config_cache_snapshot_order_fixture, crash_after_snapshot_uses_snapshot) {
    arrange_fault_after_snapshot();

    auto fut = mgr.apply_delta(make_delta_cmd(
        cluster_config_delta_cmd_data{
            .upsert = {{.key = "test_prop", .value = "true"}}}));
    EXPECT_THROW(co_await std::move(fut), simulated_fault);

    // Snapshot captured the delta:
    EXPECT_TRUE(fake_snapshot_contains("test_prop", "true"));
    // Cache did NOT (store_delta was prevented):
    EXPECT_FALSE(on_disk_cache_contains("test_prop"));

    // Reinit from snapshot:
    auto preload = co_await config_manager::preload_join(
        fake_snapshot.to_controller_join_snapshot());
    EXPECT_EQ(preload.raw_values["test_prop"], "true");
}
```

- [ ] **Step 2: Add target to BUILD.**

```python
redpanda_cc_gtest(
    name = "config_cache_snapshot_order_test",
    timeout = "short",
    srcs = ["config_cache_snapshot_order_test.cc"],
    deps = [
        "//src/v/cluster",
        "//src/v/cluster/tests:cluster_test_fixture",
        "@googletest//:gtest_main",
    ],
)
```

- [ ] **Step 3: Run.**

```
bazel test //src/v/cluster/tests:config_cache_snapshot_order_test
```

Expected: PASS.

- [ ] **Step 4: Commit.**

```bash
git add src/v/cluster/tests/config_cache_snapshot_order_test.cc \
        src/v/cluster/tests/BUILD
git commit -m "cluster: test crash-between-snapshot-and-cache recovery

Verifies that injecting a fault after force_snapshot but before
store_delta produces a state where the snapshot is authoritative and
preload from snapshot recovers the new value."
```

---

## Phase 5: Migrate the Three Motivating Properties

Each migration is a property-by-property sweep. The compiler enforces no implicit accessor, so every call site fails to build until reviewed and translated.

### Task 15: Migrate `cloud_storage_enabled`

**Files:**
- Modify: `src/v/config/configuration.h:375`
- Modify: `src/v/config/configuration.cc:2003-2013`
- Modify: ~70 call sites across `src/v/`

- [ ] **Step 1: Change the property type.**

`src/v/config/configuration.h:375`:

```cpp
enterprise<clustered_property<bool>> cloud_storage_enabled;
```

The `enterprise<>` template requires its `P` parameter to satisfy the `Property` concept (derives from `base_property`); `clustered_property<T>` does. Verify by:

```
bazel build //src/v/config:config
```

If the concept fails, inspect the `Property` concept definition (`src/v/config/property.h:1146-1150`) and confirm `clustered_property<T>` exposes `value_type`.

- [ ] **Step 2: Build the whole tree to surface call sites.**

```
bazel build //... 2>&1 | grep "cloud_storage_enabled" | head -30
```

The errors will reference `operator()` deletion. Each is a candidate for either `.local_value()` or `.value()`.

- [ ] **Step 3: Triage each call site.**

For each compile error, classify by surrounding context:

**`.local_value()`** sites (bootstrap, STM construction, materialization decisions):
- `src/v/cluster/partition.cc:67` — `partition::start()` deciding whether to construct archival STMs locally.
- `src/v/cluster/partition.cc:165,779,1222,1256` — partition lifecycle checks.
- `src/v/cluster/archival/archiver_manager.cc:549` — archiver init decision.
- `src/v/cluster/self_test/cloudcheck.cc:66` — local cloudcheck self-test.
- `src/v/cluster/feature_manager.cc:258` — feature enablement enumeration (operates from local state).
- `src/v/cluster/controller.cc:177` — controller startup decision.
- `src/v/redpanda/application.cc:904,917,924` — application startup helpers.
- `src/v/resource_mgmt/memory_groups.cc:32` — memory-group sizing.

**`.value()`** sites (cluster-uniform admission/validation):
- `src/v/cluster/topics_frontend.cc:76,138` — topic admission. Wrap with `try`/`catch (const config::config_not_converged&)` and translate to `errc::not_leader` or similar retryable error.
- `src/v/config/validators.cc:465` — config validator. Same wrap pattern.

**`.is_restricted()`** sites — already a method call, doesn't need a change. The `enterprise<>` wrapper exposes it directly:
- `src/v/cluster/topics_frontend.cc:76,138` — already use `.is_restricted()`; unaffected.

- [ ] **Step 4: Apply call-site changes in batches by directory.**

For each batch, edit, build, commit. Example for `src/v/cluster/partition.cc`:

```cpp
// Before:
if (!config::shard_local_cfg().cloud_storage_enabled()) {
    return;
}
// After:
if (!config::shard_local_cfg().cloud_storage_enabled.local_value()) {
    return;
}
```

For the `topics_frontend.cc` admission sites:

```cpp
// Before:
if (config::shard_local_cfg().cloud_storage_enabled()) {
    /* allow topic with remote.write */
}
// After:
bool cloud_enabled;
try {
    cloud_enabled = config::shard_local_cfg().cloud_storage_enabled.value();
} catch (const config::config_not_converged&) {
    co_return errc::not_leader;  // retryable
}
if (cloud_enabled) {
    /* allow topic with remote.write */
}
```

- [ ] **Step 5: Update tests.**

```
grep -rn "cloud_storage_enabled.set_value\|cloud_storage_enabled\s*=" src/v/ --include="*.cc"
```

For each test that sets `cloud_storage_enabled` directly, also set it active (avoids tests having to drive a full activation cycle):

```cpp
cfg.cloud_storage_enabled.set_value(true);
cfg.cloud_storage_enabled.test_activate(true);  // mark active for test
```

If a test setter pattern is widespread, factor into a helper:

```cpp
// In a test helper:
template<typename T>
void set_and_activate(clustered_property<T>& p, T v) {
    p.set_value(YAML::Node{v});
    p.test_activate(v);
}
```

- [ ] **Step 6: Build the whole tree.**

```
bazel build //... 2>&1 | tail -20
```

Expected: build success.

- [ ] **Step 7: Run cluster tests.**

```
bazel test //src/v/cluster/tests/...
bazel test //src/v/cluster/archival/tests/...
```

Expected: all green.

- [ ] **Step 8: Commit.**

```bash
git add -u
git commit -m "config: migrate cloud_storage_enabled to clustered_property

Migrates all ~70 call sites. Local-decision sites (partition
construction, archiver init, memory groups) use .local_value().
Cluster-admission sites (topics_frontend, validators) use .value()
with config_not_converged translated to a retryable error.
Test fixtures use test_activate() helper to skip the activation cycle."
```

### Task 16: Migrate `cloud_topics_enabled`

**Files:**
- Modify: `src/v/config/configuration.h:788`
- Modify: `src/v/config/configuration.cc` (init list entry for `cloud_topics_enabled`)
- Modify: call sites (smaller set than `cloud_storage_enabled`)

- [ ] **Step 1: Change the type and rebuild.**

`src/v/config/configuration.h:788`:

```cpp
enterprise<clustered_property<bool>> cloud_topics_enabled;
```

- [ ] **Step 2: Survey call sites.**

```
grep -rn "cloud_topics_enabled\b" --include="*.h" --include="*.cc" src/v/
```

- [ ] **Step 3: Triage and edit.**

Categorize each compile error by surrounding context:

**`.local_value()`** for sites that decide whether *this node* should materialize a subsystem locally (bootstrap, STM construction, memory-group sizing). Example:
```cpp
// Before:
if (config::shard_local_cfg().cloud_topics_enabled()) { /* init */ }
// After:
if (config::shard_local_cfg().cloud_topics_enabled.local_value()) { /* init */ }
```

**`.value()`** with try/catch for cluster-admission sites (topic creation paths that admit only when the cluster has converged):
```cpp
bool enabled;
try {
    enabled = config::shard_local_cfg().cloud_topics_enabled.value();
} catch (const config::config_not_converged&) {
    co_return errc::not_leader;  // retryable
}
```

Key sites for this property:
- `src/v/resource_mgmt/memory_groups.cc:32` — `local_value()`.
- Cluster admission paths for cloud-topic creation — `value()` with retry translation.

- [ ] **Step 4: Build and test.**

```
bazel build //...
bazel test //src/v/cluster/tests/...
```

Expected: green.

- [ ] **Step 5: Commit.**

```bash
git add -u
git commit -m "config: migrate cloud_topics_enabled to clustered_property"
```

### Task 17: Migrate `iceberg_enabled`

**Files:**
- Modify: `src/v/config/configuration.h:715`
- Modify: `src/v/config/configuration.cc:4082`
- Modify: ~50 call sites

- [ ] **Step 1: Change the type.**

`src/v/config/configuration.h:715`:

```cpp
enterprise<clustered_property<bool>> iceberg_enabled;
```

- [ ] **Step 2: Survey.**

```
grep -rn "iceberg_enabled\b" --include="*.h" --include="*.cc" src/v/
```

- [ ] **Step 3: Triage.**

Categorize each compile error the same way as Task 15:
- `.local_value()` for sites that decide whether *this node* should materialize a subsystem locally.
- `.value()` (wrapped with try/catch for `config_not_converged`) for sites that admit cluster-wide effects only when converged.

Specific guidance for `iceberg_enabled`:
- The helper at `src/v/resource_mgmt/memory_groups.cc:28` (`datalake_enabled()`) reads `iceberg_enabled`. Update it to `.local_value()`.
- Many `datalake_enabled()` call sites (e.g., in `application.cc`) are themselves local-bootstrap decisions, so the helper's `.local_value()` is correct for them.
- If any caller of the `datalake_enabled()` helper needs cluster-converged semantics (e.g., admission of a topic with iceberg.enabled=true), expose a second helper `datalake_enabled_cluster()` that calls `.value()` and propagate the throw.

- [ ] **Step 4: Build and test.**

```
bazel build //...
bazel test //src/v/...
```

- [ ] **Step 5: Commit.**

```bash
git add -u
git commit -m "config: migrate iceberg_enabled to clustered_property"
```

---

## Phase 6: ducktape Integration Tests

These tests exercise the full pipeline against a real (containerized) cluster.

### Task 18: Rolling restart test for a clustered property

**Files:**
- Create: `tests/rptest/tests/clustered_config_test.py`

- [ ] **Step 1: Write the test.**

```python
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from ducktape.utils.util import wait_until


class ClusteredConfigTest(RedpandaTest):
    def __init__(self, test_context):
        super().__init__(
            test_context=test_context, num_brokers=3,
            extra_rp_conf={"cloud_storage_enabled": False})

    @cluster(num_nodes=3)
    def test_rolling_restart_activates_clustered_property(self):
        # Initial state: cloud_storage_enabled is false everywhere.
        for node in self.redpanda.nodes:
            assert self._read_local(node, "cloud_storage_enabled") is False

        # Update the cluster config (stages but does not activate).
        self.redpanda.set_cluster_config(
            {"cloud_storage_enabled": True})

        # Before any restart: every node still reports false locally.
        for node in self.redpanda.nodes:
            assert self._read_local(node, "cloud_storage_enabled") is False
        # And the cluster-converged value should not be active.
        assert not self._is_active("cloud_storage_enabled")

        # Topic admission requiring cluster-uniform semantics must fail.
        rpk = RpkTool(self.redpanda)
        try:
            rpk.create_topic(
                "tiered_topic",
                config={"redpanda.remote.write": "true"})
            assert False, "topic create should have failed during window"
        except Exception:
            pass  # expected

        # Rolling restart.
        for node in self.redpanda.nodes:
            self.redpanda.restart_nodes([node])
            wait_until(
                lambda: self.redpanda.healthy(),
                timeout_sec=30, backoff_sec=1)

        # After rolling restart, every node has the new value locally.
        for node in self.redpanda.nodes:
            assert self._read_local(node, "cloud_storage_enabled") is True

        # And the cluster has converged.
        wait_until(
            lambda: self._is_active("cloud_storage_enabled"),
            timeout_sec=30, backoff_sec=1)

        # Topic admission now succeeds.
        rpk.create_topic(
            "tiered_topic",
            config={"redpanda.remote.write": "true"})

    def _read_local(self, node, property_name):
        # Use the admin API to read the local active value of a property.
        return self.redpanda.admin.get_local_cluster_config(node)[property_name]

    def _is_active(self, property_name):
        # Use the admin API to read cluster-converged status.
        return self.redpanda.admin.get_cluster_config_is_active(property_name)
```

- [ ] **Step 2: If the admin endpoints don't exist, add them.**

The test references `get_local_cluster_config` and `get_cluster_config_is_active`. Check `src/v/redpanda/admin/cluster_config.cc` for existing endpoints; add new ones if needed:

- `GET /v1/cluster_config/local` — returns `{property_name: local_value}` for the queried node.
- `GET /v1/cluster_config/is_active?property=NAME` — returns `{is_active: bool}`.

Each is a thin wrapper over the corresponding `clustered_property` accessor.

- [ ] **Step 3: Run the test.**

```
ducktape --debug tests/rptest/tests/clustered_config_test.py
```

Expected: PASS.

- [ ] **Step 4: Commit.**

```bash
git add tests/rptest/tests/clustered_config_test.py \
        src/v/redpanda/admin/cluster_config.cc \
        src/v/redpanda/admin/cluster_config.h
git commit -m "tests: rolling restart activates clustered_property

ducktape test exercises the full pipeline: stage, reject admission,
rolling restart, observe convergence, succeed admission. Adds two
admin endpoints for local_value and is_active read access."
```

### Task 19: ducktape test for dead-node blocking activation

**Files:**
- Modify: `tests/rptest/tests/clustered_config_test.py`

- [ ] **Step 1: Add the test.**

```python
@cluster(num_nodes=3)
def test_dead_node_blocks_activation(self):
    self.redpanda.set_cluster_config(
        {"cloud_storage_enabled": True})

    # Take down one node before rolling restart.
    self.redpanda.stop_node(self.redpanda.nodes[2])

    # Rolling restart of the remaining two.
    for node in self.redpanda.nodes[:2]:
        self.redpanda.restart_nodes([node])
        wait_until(
            lambda: self.redpanda.is_node_up(node),
            timeout_sec=30, backoff_sec=1)

    # The two live nodes have the new local_value...
    for node in self.redpanda.nodes[:2]:
        assert self._read_local(node, "cloud_storage_enabled") is True

    # ...but is_active stays false (dead node blocks).
    import time
    time.sleep(5)  # give the activation loop time to run
    assert not self._is_active("cloud_storage_enabled")

    # Bring the dead node back.
    self.redpanda.start_node(self.redpanda.nodes[2])
    wait_until(
        lambda: self.redpanda.healthy(),
        timeout_sec=60, backoff_sec=1)
    # It still has the old local_value until restarted with new config.
    # Once it restarts and reports, activation completes.
    wait_until(
        lambda: self._is_active("cloud_storage_enabled"),
        timeout_sec=30, backoff_sec=1)
```

- [ ] **Step 2: Run.**

```
ducktape --debug tests/rptest/tests/clustered_config_test.py::ClusteredConfigTest.test_dead_node_blocks_activation
```

Expected: PASS.

- [ ] **Step 3: Commit.**

```bash
git add tests/rptest/tests/clustered_config_test.py
git commit -m "tests: dead-node blocks clustered_property activation

Verifies the operational semantics: a dead/partitioned node prevents
is_active from flipping true, mirroring feature_manager's behavior."
```

---

## Done

After Task 19, the implementation is complete:

- `clustered_property<T>` type with `local_value()`, `value()` (throws), `is_active()`, `wait_until_active()`.
- STM command for cluster-wide activation.
- Activation loop in `config_manager` mirroring `feature_manager`.
- Cache↔snapshot ordering invariant.
- Three properties migrated.
- Unit, integration, and ducktape coverage.

**Verification commands to run before merge:**

```bash
bazel build //...
bazel test //src/v/config/...
bazel test //src/v/cluster/...
ducktape --debug tests/rptest/tests/clustered_config_test.py
bazel run //tools:clang_format
```
