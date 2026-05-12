# Clustered Config Properties: Design

**Status:** Design — pending implementation plan
**Branch:** `config_invariant_fixes`
**Date:** 2026-05-12

## Motivation

Some Redpanda config properties gate subsystems (STMs, partition machinery) that
materialize on every broker. Today these are declared `needs_restart::yes`,
which gives each broker a (active, pending) value pair that is promoted on
restart. The split is per-node: nothing prevents two brokers from running with
divergent active values during a rolling restart.

For most restart-required properties that divergence is tolerable — the
property's effect is local. For subsystem-gating properties it is not:
`cloud_storage_enabled`, `cloud_topics_enabled`, and `iceberg_enabled` (exposed
through the `datalake_enabled()` helper in `src/v/resource_mgmt/memory_groups.cc`)
control whether tiered storage, cloud topics, and the datalake subsystem
materialize on the broker. If the leader believes one of these is enabled
cluster-wide and admits a topic that depends on it, but a follower hasn't yet
restarted, the follower can't materialize the required STMs and the partition
is broken on that replica.

This design introduces a property variant that distinguishes the node-local
view (used for bootstrap decisions) from the cluster-converged view (used for
admission decisions), with a convergence gate modelled on the existing
`feature_manager`.

## Approach Overview

Two coupled mechanisms:

1. **A new property type, `clustered_property<T>`,** that exposes
   `local_value()` (node-local, no throw) and `value()` (cluster-converged,
   throws if a pending update is not yet ratified across all members). Backed
   by a convergence detector inside `config_manager` that mirrors the
   feature_manager activation pattern.
2. **An invariant: the on-disk config cache offset is never ahead of the
   latest controller-STM-snapshot offset.** Enforced by forcing a controller
   snapshot before writing the cache on every config delta.

The two halves are coupled because the convergence detector reads STM state to
determine "what value are we converging on," and a cache that can drift past
the snapshot offset would let a recovering broker seed `_local_active` with a
value that no longer exists in the cluster's authoritative log.

## Design Principle

**Reuse `feature_manager` logic where structurally possible.** The activation
pattern (health-monitor callback → per-member version map → background loop
on the leader → STM command on convergence → idempotent apply on each replica)
is already in production. The implementation should extract or mirror its
mechanics rather than re-deriving them. Specific reuse targets:

- `health_monitor_backend::register_node_callback` callback wiring.
- `members_table` iteration + `health_monitor_frontend::is_alive` gating.
- Per-node value map cleared on leadership change, pre-populated for self and
  founder nodes.
- Background fiber pattern (`ssx::spawn_with_gate_then` + `ss::do_until`).

## Part 1: The `clustered_property<T>` Type

### API

```cpp
template<typename T>
class clustered_property : public property<T> {
public:
    /// The active value as known by THIS node. Reflects the last value the
    /// node was bootstrapped/restarted with. No throw. Use during bootstrap
    /// and for local-only decisions (e.g., whether to materialize an STM).
    T local_value() const;

    /// The cluster-converged value. Throws config_not_converged if a pending
    /// update has not yet been ratified across all alive members. Use for
    /// decisions that require cluster-uniform semantics (e.g., topic
    /// admission, schema validation).
    T value() const;  // throws

    /// True iff there is no staged update OR all alive members have
    /// converged on the staged value as their local_active, AND the
    /// controller has replicated the activation command.
    bool is_active() const;

    /// Resolves once is_active() becomes true, then returns value().
    /// Abort-source aware so it unblocks during shutdown. Callers needing a
    /// bounded wait wrap with ss::with_timeout.
    ss::future<T> wait_until_active(ss::abort_source&) const;

    // No operator() — clustered properties force explicit choice.
};

class config_not_converged : public std::runtime_error { /* ... */ };
```

Removing the implicit `operator()` is deliberate: the compiler forces every
existing call site to be triaged during migration, and new callers cannot
accidentally pick the wrong semantics.

`clustered_property<T>` derives from the existing `property<T>` and returns
`needs_restart::yes` from its `needs_restart()` getter. From the perspective
of `config_manager`, admin tooling, and `rpk`, a clustered property is a
restart-required property; the convergence gate is internal.

### Per-Property State

In addition to the `(active, pending)` slots from the base `property<T>`:

- `_staged: std::optional<T>` — cluster-staged value awaiting convergence.
  Set by `update_delta_cmd`, cleared by `config_activate_cmd`. STM-replicated;
  survives node restarts (unlike `_pending`, which is per-node and is promoted
  on restart).
- `_is_active: bool` — set to `true` by `config_activate_cmd`; set to `false`
  by `update_delta_cmd` whenever it stages a new value. The new-update path
  invalidates activeness immediately; activeness is only restored after the
  cluster has converged on the new staged value.

The existing `_pending` continues to be used per-node for the restart-promotion
mechanic. For a clustered property, both `_pending` (per-node, transient) and
`_staged` (cluster, durable until activation) are populated at the same time;
they decouple after a node restart locally promotes its `_pending`.

### `config_not_converged` Failure Mode

Derived from `std::runtime_error` so any handler can catch it without
introducing a new exception family. Callers that gate cluster-wide effects
(topic creation, validators) translate it to a retryable error code at the API
boundary (`BROKER_NOT_AVAILABLE` for Kafka admission paths; a 503-equivalent
for admin endpoints).

## Part 2: Convergence Detection

### Health Monitor Extension

`node_health_report` gains:

```cpp
absl::flat_hash_map<ss::sstring, iobuf> clustered_config;
```

Each entry is `{property_name, serialized local_active}` for every clustered
property. Payload is small: a handful of properties with primitive values.
Piggybacks on the existing health-report fan-in to the controller leader.

### Activation Loop in `config_manager`

Mirrors `feature_manager::maybe_update_feature_table`
(`src/v/cluster/feature_manager.cc:605-657`):

- Runs only on the leader. Rebuilt on leadership change.
- For each clustered property `p` with `p._staged.has_value()` and
  `p._is_active == false`:
  - Iterate every member from `members_table.node_ids()` (not "alive nodes
    from health monitor" — every member).
  - For each member: require an entry in the leader's `_node_local_values`
    map (populated by the health-report callback). If unknown → defer
    (`co_return`, retry on next loop iteration).
  - AND `_hm_frontend.is_alive(node_id) == alive::yes`. If not alive → throw
    `std::runtime_error` to trigger backoff + retry (same pattern as
    `feature_manager.cc:641,650`).
  - AND each member's reported `local_active == p._staged`.
- When the predicate passes for `p`: replicate
  `config_activate_cmd{property_name, staged_value}` via the controller STM.

On leadership change, clear `_node_local_values` (matches
`feature_manager.cc:141`); pre-populate the leader's own entry
(`:168-170`) and the founder nodes' entries (`:199-201`) at startup to avoid
deadlock on a fresh cluster.

Dead/partitioned nodes block activation until resolved. Operators must bring
the node back online or decommission it to unblock. This is the same
operational model as feature activation.

### `config_activate_cmd` STM Command

- Key: the next available key in `config_manager`'s STM command set
  (today's commands are defined alongside `update_delta_cmd`).
- Body: `{property_name: ss::sstring, value: iobuf}`.
- Apply (on every replica, idempotent):
  - If the named property has `_staged == decoded_value`: set
    `_is_active = true`, clear `_staged`, signal the
    `wait_until_active` condition variable.
  - If `_staged` does not match (a newer `update_delta_cmd` has overwritten
    it in flight): drop the activation. The new staged value gets its own
    activation cycle.

### Edge Cases

- **Fresh cluster bootstrap.** No staged updates; defaults are unanimous;
  `_is_active = true` from genesis. Founder-node pre-population
  (mirroring `feature_manager.cc:199-201`) prevents a bootstrap deadlock.
- **Update overwrites in-flight update.** `update_delta_cmd` replaces
  `_staged` atomically. Existing convergence tracking is abandoned; the new
  staged value gets its own activation. Restart-pending promotion follows
  the latest staged value.
- **Leader change mid-window.** STM state has `_staged` populated and
  `_is_active=false`. New leader resumes the activation check from its own
  health monitor; idempotent.
- **New node joins mid-window.** Joiner installs the snapshot with `_staged`
  and `_is_active` already set consistently. On its first restart since
  joining, promotes locally. Counts in the activation check once it sends
  a health report.
- **Network partition.** Partitioned nodes drop the `is_alive` check;
  activation throws and retries, blocking promotion until the partition
  heals. Same trade-off `feature_manager` accepts.

## Part 3: Cache↔Snapshot Invariant

### Rule

After `config_manager` applies any `update_delta_cmd` (clustered or
otherwise), it forces a controller STM snapshot **before** writing the
config cache. After settlement, `cache_offset <= latest_snapshot_offset`
always holds.

### Ordering

1. Apply the delta in-memory (existing `apply_delta`).
2. Force a controller STM snapshot.
3. Write the config cache (existing `store_delta`).

Crash analysis:

- **Crash between steps 2 and 3.** Snapshot is current; cache is stale.
  Preload seeds from old cache; STM replay catches up to the snapshot.
  Invariant `cache_offset <= snapshot_offset` holds.
- **Crash after step 3.** Snapshot and cache both current. Invariant holds.
- **Never possible:** cache contains updates not in the snapshot.

### Mechanism

`config_manager::apply_delta` requests a snapshot via the controller's
existing snapshot path. The implementation plan will need to verify whether
that path exposes an on-demand API and add one if not; this is a known
follow-up to validate during implementation, not a separate design.

### Cost

Config changes are infrequent (admin-driven, not workload-driven). Multiple
deltas in quick succession can coalesce into a single snapshot
(one snapshot per burst is sufficient — correctness depends on "one snapshot
covering all of them, taken after the last," not "one per delta"). Total
overhead is in the noise compared to other per-config-update work.

### Interaction with Part 2

The activation loop reads `_staged` from STM state. With this invariant, a
broker recovering from snapshot sees the same `_staged` value that was
authoritative at the snapshot offset — no risk of the cache seeding a
ghost staged value. A joiner installs the snapshot, gets `_staged` and
`_is_active` consistently, and begins reporting `local_active` via the
health monitor.

## Part 4: Migration

### Properties

Three properties become clustered:

- `cloud_storage_enabled` — `src/v/config/configuration.h:375`
- `cloud_topics_enabled` — `src/v/config/configuration.h:788`
- `iceberg_enabled` — exposed via `datalake_enabled()` helper in
  `src/v/resource_mgmt/memory_groups.cc:28`

All three are wrapped in `enterprise<property<bool>>` today; they become
`enterprise<clustered_property<bool>>`. The `enterprise<>` wrapper composes
with the new type (CRTP-style; verify during implementation).

### Call Site Sweep

The compiler will break ~120 call sites (no implicit accessor). Each must
be triaged:

- **`.local_value()`** — bootstrap, STM construction, memory-group sizing,
  partition construction (`src/v/cluster/partition.cc:67,165`,
  `src/v/cluster/archival/archiver_manager.cc:549`, etc.), application
  startup helpers. Pattern: "this node is materializing a subsystem
  locally; should it."
- **`.value()`** — topic admission
  (`src/v/cluster/topics_frontend.cc:76,138`), validators
  (`src/v/config/validators.cc:465`). Pattern: "I'm making a decision the
  cluster has to honor uniformly." Wrap to translate
  `config_not_converged` into a retryable error at the API boundary.
- **`.wait_until_active()`** — admin API endpoints that explicitly trigger
  a feature and can tolerate latency. Wrapped with `ss::with_timeout` as
  needed.

### Test Helpers

Existing test fixtures (e.g.,
`src/v/cluster/archival/tests/service_fixture.cc:53`) use
`cfg.cloud_storage_enabled.set_value(true)`. For a clustered property,
seeding the new state should also flip `_is_active = true` so tests don't
need to drive a full activation cycle. A test-only setter
(`set_value_and_activate(...)`) avoids this boilerplate.

## Part 5: Testing

1. **Unit tests for `clustered_property<T>`:** throw semantics on `value()`,
   `wait_until_active()` resolution, `is_active()` transitions on STM-command
   application, promotion semantics across restart.
2. **`gtest` integration tests for the activation loop in `config_manager`:**
   full activation loop with a mocked health monitor, including
   leader-change, dead-node, joiner, and update-overwrites-in-flight
   scenarios. Mirror the existing `feature_manager_test` patterns.
3. **ducktape tests** (`tests/rptest/`): real-cluster rolling restart with
   `cloud_storage_enabled` toggled. Verify `is_active()` flips after the
   rolling restart completes. Verify topic create with `redpanda.remote.write=true`
   is rejected during the convergence window and succeeds after.
4. **Crash test for cache↔snapshot ordering:** inject a fault between
   snapshot write and cache write; verify recovery uses the snapshot value,
   not a divergent cache value.

## Out of Scope

- Migrating other restart-required properties to `clustered_property`.
  Future work, same mechanism, no design change.
- Per-node config overrides for clustered properties. Clustered properties
  are inherently cluster-uniform; per-node overrides would defeat the
  purpose.
- New admin-facing UI/CLI surfaces. `rpk` and the admin API see clustered
  properties as ordinary restart-required properties (the `needs_restart()`
  getter still returns `needs_restart::yes`).

## Open Questions

- **On-demand snapshot trigger.** Whether the existing controller-snapshot
  path already exposes an on-demand API, or whether one must be added.
  Resolved during implementation.
- **Default flip across Redpanda versions.** If a clustered property's
  default changes in a new binary version, that is effectively an update
  from the old default to the new default. The cluster will see divergent
  `local_active` during a binary upgrade rolling restart, which is
  precisely the convergence window the design handles — but the
  `_staged` value isn't set by any `update_delta_cmd` in this case. The
  implementation may need to seed `_staged` from the new default on
  startup when local_active disagrees. Flagging for follow-up.

