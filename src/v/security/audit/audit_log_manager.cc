/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "security/audit/audit_log_manager.h"

#include "client.h"
#include "cluster/controller.h"
#include "cluster/metadata_cache.h"
#include "config/configuration.h"
#include "config/types.h"
#include "features/feature_table.h"
#include "kafka/data/record_batcher.h"
#include "kafka/data/rpc/client.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "security/acl.h"
#include "security/audit/logger.h"
#include "security/audit/schemas/application_activity.h"
#include "security/audit/schemas/types.h"
#include "security/audit/schemas/utils.h"
#include "ssx/semaphore.h"

#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <algorithm>
#include <memory>
#include <optional>

namespace security::audit {

/// audit_log_manager

void audit_log_manager::set_enabled_events() {
    using underlying_enum_t = std::underlying_type_t<event_type>;
    _enabled_event_types = underlying_enum_t(0);
    for (const auto& e : _audit_event_types()) {
        const auto as_uint = underlying_enum_t(string_to_event_type(e));
        _enabled_event_types[as_uint] = true;
    }
    vassert(
      !is_audit_event_enabled(event_type::unknown),
      "Unknown event_type observed");
}

bool audit_log_manager::recovery_mode_enabled() noexcept {
    return config::node().recovery_mode_enabled.value();
}

audit_log_manager::audit_log_manager(
  model::node_id self,
  cluster::controller* controller,
  ss::sharded<cluster::metadata_cache>* metadata_cache,
  ss::sharded<kafka::data::rpc::client>* rpc_client,
  kafka::client::configuration& cfg)
  : _audit_enabled(config::shard_local_cfg().audit_enabled.bind())
  , _audit_log_reject_policy(
      config::shard_local_cfg().audit_failure_policy.bind())
  , _queue_drain_interval_ms(
      config::shard_local_cfg().audit_queue_drain_interval_ms.bind())
  , _audit_event_types(
      config::shard_local_cfg().audit_enabled_event_types.bind())
  , _max_queue_size_bytes(
      config::shard_local_cfg().audit_queue_max_buffer_size_per_shard())
  , _audit_excluded_topics_binding(
      config::shard_local_cfg().audit_excluded_topics.bind())
  , _audit_excluded_principals_binding(
      config::shard_local_cfg().audit_excluded_principals.bind())
  , _queue_bytes_sem(_max_queue_size_bytes, "s/audit/buffer")
  , _self(self)
  , _controller(controller)
  , _config(&cfg)
  , _metadata_cache(metadata_cache)
  , _rpc_client(rpc_client) {
    _probe.setup_metrics([this] {
        return 1.0
               - (static_cast<double>(_queue_bytes_sem.available_units()) / static_cast<double>(_max_queue_size_bytes));
    });
    set_enabled_events();
    _audit_event_types.watch([this] { set_enabled_events(); });
    _audit_excluded_topics_binding.watch([this] {
        _audit_excluded_topics.clear();
        const auto& excluded_topics = _audit_excluded_topics_binding();
        std::for_each(
          excluded_topics.cbegin(),
          excluded_topics.cend(),
          [this](const ss::sstring& topic) {
              _audit_excluded_topics.emplace(topic);
          });
    });
    _audit_excluded_principals_binding.watch([this] {
        _audit_excluded_principals.clear();
        const auto& excluded_principals = _audit_excluded_principals_binding();
        std::for_each(
          excluded_principals.cbegin(),
          excluded_principals.cend(),
          [this](const ss::sstring& principal) {
              if (principal.starts_with("User:")) {
                  _audit_excluded_principals.emplace(
                    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
                    security::principal_type::user,
                    principal.substr(5));
              } else {
                  _audit_excluded_principals.emplace(
                    security::principal_type::user, principal);
              }
          });
    });
}

audit_log_manager::~audit_log_manager() = default;

bool audit_log_manager::is_audit_event_enabled(event_type event_type) const {
    using underlying_enum_t
      = std::underlying_type_t<security::audit::event_type>;
    return _enabled_event_types.test(underlying_enum_t(event_type));
}

ss::future<> audit_log_manager::start() {
    if (recovery_mode_enabled()) {
        vlog(
          adtlog.warn,
          "Redpanda is operating in recovery mode.  Auditing is disabled!");
        co_return;
    }

    // NOTE: construct a sink on every shard, unconditionally. the kafka
    // sinks on the non-client shards will be inactive.
    const bool internal_rpcs_available
      = _controller->get_feature_table().local().get_active_version()
        >= features::to_cluster_version(features::release_version::v25_3_1);
    if (config::shard_local_cfg().audit_use_rpc() && internal_rpcs_available) {
        vlog(adtlog.info, "Audit log in RPC mode");
        _sink = audit_sink::make_rpc_sink(
          this, _controller, &_rpc_client->local());
    } else {
        if (
          config::shard_local_cfg().audit_use_rpc() && !internal_rpcs_available
          && ss::this_shard_id() == 0) {
            vlog(
              adtlog.warn,
              "Audit log RPC mode requested but not available cluster-wide. "
              "Falling back to Kafka Client mode. RPC mode will be available "
              "on the next restart after all brokers are upgraded to v25.3.1+");
        }
        vlog(adtlog.info, "Audit log in Kafka Client mode");
        _sink = audit_sink::make_kafka_sink(
          this, _controller, *_config, [this](bool v) {
              return container().invoke_on_all(
                [v](audit_log_manager& mgr) { mgr.set_auth_misconfigured(v); });
          });
    }

    _drain_queue.emplace(
      ss::current_scheduling_group(), [this](const std::exception_ptr& e) {
          vlog(adtlog.warn, "Exception in audit_log_manager fiber: {}", e);
          probe().audit_error();
      });

    _audit_enabled.watch([this] {
        try {
            sink().toggle(_audit_enabled());
        } catch (...) {
            vlog(
              adtlog.error,
              "Failed to toggle auditing state: {}",
              std::current_exception());
        }
    });
    if (_audit_enabled()) {
        vlog(adtlog.info, "Starting audit_log_manager");
        co_await sink().start();
    }
}

ss::future<> audit_log_manager::stop() {
    _as.request_abort();
    if (_drain_as) {
        _drain_as->request_abort();
    }
    vlog(adtlog.info, "Shutting down audit log manager");
    if (_sink) {
        co_await sink().stop();
    }
    if (_drain_queue) {
        co_await _drain_queue->shutdown();
    }
    if (_queue.size() > 0) {
        vlog(
          adtlog.debug,
          "{} records were not pushed to the audit log before shutdown",
          _queue.size());
    }
}

ss::future<> audit_log_manager::scheduled_drain() {
    auto slept = co_await ss::coroutine::as_future(
      ss::sleep_abortable(_queue_drain_interval_ms(), *_drain_as));
    if (slept.failed()) {
        /// pause() or stop() ended this chain; resume() starts a fresh one
        slept.ignore_ready_future();
        co_return;
    }
    auto drained = co_await ss::coroutine::as_future(drain());
    if (drained.failed()) {
        auto e = drained.get_exception();
        vlog(adtlog.warn, "Exception in audit_log_manager fiber: {}", e);
        probe().audit_error();
    }
    if (_drain_as->abort_requested()) {
        co_return;
    }
    // Re-queue the next drain operation.
    _drain_queue->submit([this] { return scheduled_drain(); });
}

ss::future<> audit_log_manager::quiesce_drain_queue() {
    ss::promise<> barrier;
    auto reached = barrier.get_future();
    _drain_queue->submit([barrier = std::move(barrier)]() mutable {
        barrier.set_value();
        return ss::now();
    });
    auto f = co_await ss::coroutine::as_future(std::move(reached));
    f.ignore_ready_future();
}

ss::future<> audit_log_manager::pause() {
    _effectively_enabled = false;
    if (_drain_as) {
        _drain_as->request_abort();
    }
    co_await quiesce_drain_queue();
    co_await drain(is_final_flush::yes)
      .handle_exception([this](const std::exception_ptr& e) {
          vlog(adtlog.warn, "Exception in audit_log_manager fiber: {}", e);
          probe().audit_error();
      });
}

ss::future<> audit_log_manager::resume() {
    // NOTE(oren): in kafka API mode this function is called on all shards
    // internally to the sink
    _effectively_enabled = true;
    _drain_as.emplace();
    _drain_queue->submit([this] { return scheduled_drain(); });
    return ss::now();
}

bool audit_log_manager::report_redpanda_app_event(is_started app_started) {
    return enqueue_app_lifecycle_event(
      app_started == is_started::yes
        ? application_lifecycle::activity_id::start
        : application_lifecycle::activity_id::stop);
}

// TODO: this whole function should be able to use the rpc client
// in which case we might not need to embed a metadata cache
// Could do this even if we're not using RPC for transport
model::partition_id audit_log_manager::compute_partition_id() {
    static thread_local model::partition_id _next_pid{0};

    model::topic_namespace_view ns_tp{model::kafka_audit_logging_nt};
    auto cfg = _metadata_cache->local().get_topic_cfg(ns_tp);
    if (!cfg.has_value()) {
        vlog(
          adtlog.debug,
          "{} missing from metadata cache, fall back to round-robin candidate "
          "{}",
          ns_tp,
          _next_pid);
        return _next_pid;
    }
    auto n_partitions = cfg.value().partition_count;
    vassert(n_partitions >= 0, "Invalid partition count {}", n_partitions);

    auto inc_pid = [n_partitions](model::partition_id pid, int32_t inc = 1) {
        return model::partition_id{(pid + inc) % n_partitions};
    };

    const auto& partition_leaders
      = _controller->get_partition_leaders().local();

    std::optional<model::partition_id> pid;
    for (auto i : boost::irange(n_partitions)) {
        auto try_pid = inc_pid(_next_pid, i);
        auto leader = partition_leaders.get_leader(ns_tp, try_pid);
        if (!leader.has_value()) {
            continue;
        } else if (leader.value() == _self) {
            vlog(adtlog.debug, "Node {} leads partition {}", _self, try_pid);
            pid.emplace(try_pid);
            break;
        }
    }

    // NOTE(oren): sort of arbitrary. if we didn't find a locally led partition,
    // then at least advance the round robin to the next PID in natural order
    _next_pid = inc_pid(pid.value_or(_next_pid));
    return pid.value_or(_next_pid);
}

ss::future<> audit_log_manager::drain(is_final_flush final_flush) {
    if (_queue.empty()) {
        co_return;
    }

    vlog(
      adtlog.debug,
      "Attempting to drain {} audit events from sharded queue",
      _queue.size());

    /// Combine all queued audit msgs into record_batches
    kafka::data::record_batcher batcher{
      config::shard_local_cfg().kafka_batch_max_bytes(), &adtlog};

    auto records = std::exchange(_queue, underlying_t{});
    auto& records_seq = records.get<underlying_list>();
    while (!records_seq.empty()) {
        auto first = records_seq.extract(records_seq.begin());
        auto audit_msg = std::move(first.value()).release();
        auto as_json = audit_msg->to_json();
        iobuf b;
        b.append(as_json.c_str(), as_json.size());
        batcher.append(std::nullopt, std::move(b));

        co_await ss::coroutine::maybe_yield();
    }

    auto batches = std::move(batcher).finish();

    chunked_vector<partition_batch> p_batches;
    p_batches.reserve(batches.size());

    // attach a partition ID to each batch and call into the audit_sink

    std::transform(
      std::make_move_iterator(batches.begin()),
      std::make_move_iterator(batches.end()),
      std::back_inserter(p_batches),
      [this](model::record_batch recs) {
          return partition_batch{
            .pid = compute_partition_id(),
            .batch = std::move(recs),
          };
      });

    /// This call may block if the audit_clients semaphore is exhausted,
    /// this represents the amount of memory used within its kafka::client
    /// produce batch queue. If the semaphore blocks it will apply
    /// backpressure here, and the \ref _queue will begin to fill closer to
    /// capacity. When it hits capacity, enqueue_audit_event() will block.
    auto timeout = final_flush == is_final_flush::yes
                     ? std::make_optional(final_flush_timeout)
                     : std::nullopt;
    /// Only the periodic drain is abortable, and only ever by the abort_source
    /// belonging to the shard whose client is being produced to
    const auto abortable = final_flush == is_final_flush::no;
    if (sink().active()) {
        co_await sink().produce(
          std::move(p_batches),
          timeout,
          abortable && _drain_as.has_value() ? &*_drain_as : nullptr);
    } else {
        co_await container().invoke_on(
          client_shard_id,
          [data = std::move(p_batches), timeout, abortable](
            audit_log_manager& mgr) mutable {
              return mgr.sink().produce(
                std::move(data),
                timeout,
                abortable && mgr._drain_as.has_value() ? &*mgr._drain_as
                                                       : nullptr);
          });
    }
}

audit_sink& audit_log_manager::sink() {
    vassert(_sink != nullptr, "Sink is null");
    return *_sink;
}

std::optional<audit_log_manager::audit_event_passthrough>
audit_log_manager::should_enqueue_audit_event() const {
    if (recovery_mode_enabled() || !_audit_enabled()) {
        return std::make_optional(audit_event_passthrough::yes);
    }
    if (_as.abort_requested()) {
        /// Prevent auditing new messages when shutdown starts that way the
        /// queue may be entirely flushed before shutdown
        return std::make_optional(
          _audit_log_reject_policy() == config::audit_failure_policy::reject
            ? audit_event_passthrough::no
            : audit_event_passthrough::yes);
    }
    const auto& feature_table = _controller->get_feature_table();
    if (!feature_table.local().is_active(features::feature::audit_logging)) {
        vlog(
          adtlog.warn,
          "Audit message passthrough active until cluster has been fully "
          "upgraded to the min supported version for audit_logging");
        _probe.audit_error();
        return std::make_optional(audit_event_passthrough::yes);
    }
    // NOTE: RPC client will never set this flag
    if (_auth_misconfigured) {
        /// Audit logging depends on having auth enabled, if it is not
        /// then messages are rejected for increased observability into why
        /// things are not working.
        vlog(
          adtlog.warn,
          "Audit message rejected due to misconfigured authorization");
        return std::make_optional(
          _audit_log_reject_policy() == config::audit_failure_policy::reject
            ? audit_event_passthrough::no
            : audit_event_passthrough::yes);
    }
    return std::nullopt;
}

std::optional<audit_log_manager::audit_event_passthrough>
audit_log_manager::should_enqueue_audit_event(
  event_type type, ignore_enabled_events ignore_events) const {
    if (
      ignore_events == ignore_enabled_events::no
      && !is_audit_event_enabled(type)) {
        return std::make_optional(audit_event_passthrough::yes);
    }
    return should_enqueue_audit_event();
}

namespace {
bool is_ignored_ephemeral_user(const security::acl_principal& principal) {
    return principal == security::audit_principal
           || principal == security::schema_registry_principal;
}
} // namespace

std::optional<audit_log_manager::audit_event_passthrough>
audit_log_manager::should_enqueue_audit_event(
  event_type type,
  const security::acl_principal& principal,
  ignore_enabled_events ignore_events) const {
    if (
      _audit_excluded_principals.contains(principal)
      || is_ignored_ephemeral_user(principal)) {
        return std::make_optional(audit_event_passthrough::yes);
    }

    return should_enqueue_audit_event(type, ignore_events);
}

std::optional<audit_log_manager::audit_event_passthrough>
audit_log_manager::should_enqueue_audit_event(
  kafka::api_key key,
  const security::acl_principal& principal,
  ignore_enabled_events ignore_events) const {
    return should_enqueue_audit_event(
      kafka_api_to_event_type(key), principal, ignore_events);
}

std::optional<audit_log_manager::audit_event_passthrough>
audit_log_manager::should_enqueue_audit_event(
  event_type type, const ss::sstring& username) const {
    return should_enqueue_audit_event(
      type, security::acl_principal{security::principal_type::user, username});
}

std::optional<audit_log_manager::audit_event_passthrough>
audit_log_manager::should_enqueue_audit_event(
  event_type type, const security::audit::user& user) const {
    auto principal_type = user.type_id == audit::user::type::system
                            ? security::principal_type::ephemeral_user
                            : security::principal_type::user;

    return should_enqueue_audit_event(
      type, security::acl_principal{principal_type, user.name});
}

std::optional<audit_log_manager::audit_event_passthrough>
audit_log_manager::should_enqueue_audit_event(
  kafka::api_key key,
  const security::acl_principal& principal,
  const model::topic& t) const {
    auto ignore_events = ignore_enabled_events::no;
    if (_audit_excluded_topics.contains(t)) {
        return std::make_optional(audit_event_passthrough::yes);
    } else if (t == model::kafka_audit_logging_topic) {
        ignore_events = ignore_enabled_events::yes;
    }

    return should_enqueue_audit_event(key, principal, ignore_events);
}

} // namespace security::audit
