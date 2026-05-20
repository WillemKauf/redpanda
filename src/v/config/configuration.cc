// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/configuration.h"

#include "base/units.h"
#include "cluster/scheduling/topic_memory_per_partition_default.h"
#include "config/base_property.h"
#include "config/bounded_property.h"
#include "config/sasl_mechanisms.h"
#include "config/types.h"
#include "config/validators.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "net/tls.h"
#include "security/config.h"
#include "security/oidc_url_parser.h"
#include "serde/rw/chrono.h"
#include "ssx/sformat.h"
#include "storage/config.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>

#include <chrono>
#include <cstdint>
#include <optional>

namespace config {
using namespace std::chrono_literals;

configuration::configuration()
  : log_segment_size(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "log_segment_size",
            .desc = "Default log segment size in bytes for topics which do not "
                    "set segment.bytes",
            .needs_restart = needs_restart::no,
            .example = "2147483648",
            .visibility = visibility::tunable,
          };
      }),
      128_MiB,
      {.min = 1_MiB})
  , log_segment_size_min(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "log_segment_size_min",
            .desc = "Lower bound on topic `segment.bytes`: lower values will "
                    "be clamped to this limit.",
            .needs_restart = needs_restart::no,
            .example = "16777216",
            .visibility = visibility::tunable,
          };
      }),
      1_MiB)
  , log_segment_size_max(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "log_segment_size_max",
            .desc = "Upper bound on topic `segment.bytes`: higher values will "
                    "be clamped to this limit.",
            .needs_restart = needs_restart::no,
            .example = "268435456",
            .visibility = visibility::tunable,
          };
      }),
      std::nullopt)
  , log_segment_size_jitter_percent(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "log_segment_size_jitter_percent",
            .desc = "Random variation to the segment size limit used for each "
                    "partition.",
            .needs_restart = needs_restart::yes,
            .example = "2",
            .visibility = visibility::tunable,
          };
      }),
      5,
      {.min = 0, .max = 99})
  , compacted_log_segment_size(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "compacted_log_segment_size",
            .desc = "Size (in bytes) for each compacted log segment.",
            .needs_restart = needs_restart::no,
            .example = "268435456",
            .visibility = visibility::tunable,
          };
      }),
      256_MiB,
      {.min = 1_MiB})
  , readers_cache_eviction_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "readers_cache_eviction_timeout_ms",
            .desc
            = "Duration after which inactive readers are evicted from cache.",
            .visibility = visibility::tunable,
          };
      }),
      30s)
  , readers_cache_target_max_size(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "readers_cache_target_max_size",
            .desc
            = "Maximum desired number of readers cached per NTP. This a soft "
              "limit, meaning that a number of readers in cache may "
              "temporarily increase as cleanup is performed in the background.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      200,
      {.min = 0, .max = 10000})
  , log_segment_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "log_segment_ms",
            .desc
            = "Default lifetime of log segments. If `null`, the property is "
              "disabled, and no default lifetime is set. Any value under 60 "
              "seconds (60000 ms) is rejected. This property can also be set "
              "in the Kafka API using the Kafka-compatible alias, "
              "`log.roll.ms`. The topic property `segment.ms` overrides the "
              "value of `log_segment_ms` at the topic level.",
            .needs_restart = needs_restart::no,
            .example = "3600000",
            .visibility = visibility::user,
          };
      }),
      std::chrono::weeks{2},
      {.min = 60s})
  , log_segment_ms_min(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "log_segment_ms_min",
            .desc = "Lower bound on topic `segment.ms`: lower values will be "
                    "clamped to this value.",
            .needs_restart = needs_restart::no,
            .example = "600000",
            .visibility = visibility::tunable,
          };
      }),
      10min,
      {.min = 0ms})
  , log_segment_ms_max(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "log_segment_ms_max",
            .desc = "Upper bound on topic `segment.ms`: higher values will be "
                    "clamped to this value.",
            .needs_restart = needs_restart::no,
            .example = "31536000000",
            .visibility = visibility::tunable,
          };
      }),
      24h * 365,
      {.min = 0ms})
  , rpc_server_listen_backlog(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "rpc_server_listen_backlog",
            .desc = "Maximum TCP connection queue length for Kafka server and "
                    "internal RPC server. If `null` (the default value), no "
                    "queue length is set.",
            .visibility = visibility::user,
          };
      }),
      std::nullopt,
      {.min = 1})
  , rpc_server_tcp_recv_buf(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "rpc_server_tcp_recv_buf",
            .desc = "Internal RPC TCP receive buffer size. If `null` (the "
                    "default value), no buffer size is set by Redpanda.",
            .example = "65536",
          };
      }),
      std::nullopt,
      {.min = 32_KiB, .align = 4_KiB})
  , rpc_server_tcp_send_buf(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "rpc_server_tcp_send_buf",
            .desc = "Internal RPC TCP send buffer size. If `null` (the default "
                    "value), then no buffer size is set by Redpanda.",
            .example = "65536",
          };
      }),
      std::nullopt,
      {.min = 32_KiB, .align = 4_KiB})
  , rpc_client_connections_per_peer(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "rpc_client_connections_per_peer",
            .desc = "The maximum number of connections a broker will open to "
                    "each of its peers.",
            .example = "8",
          };
      }),
      128,
      {.min = 8})
  , rpc_server_compress_replies(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "rpc_server_compress_replies",
            .desc = "Enable compression for internal RPC (remote procedure "
                    "call) server replies.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , data_transforms_enabled(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "data_transforms_enabled",
            .desc
            = "Enables WebAssembly-powered data transforms directly in the "
              "broker. When `data_transforms_enabled` is set to `true`, "
              "Redpanda reserves memory for data transforms, even if no "
              "transform functions are currently deployed. This memory "
              "reservation ensures that adequate resources are available for "
              "transform functions when they are needed, but it also means "
              "that some memory is allocated regardless of usage.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
          };
      }),
      false)
  , data_transforms_commit_interval_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "data_transforms_commit_interval_ms",
            .desc = "The commit interval at which data transforms progress.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      3s)
  , data_transforms_per_core_memory_reservation(
      *this,
      static_metadata([] {
          static constexpr std::string_view _aliases[] = {
            "wasm_per_core_memory_reservation"};
          return base_property::metadata{
            .name = "data_transforms_per_core_memory_reservation",
            .desc
            = "The amount of memory to reserve per core for data transform "
              "(Wasm) virtual machines. Memory is reserved on boot. The "
              "maximum number of functions that can be deployed to a cluster "
              "is equal to `data_transforms_per_core_memory_reservation` / "
              "`data_transforms_per_function_memory_limit`.",
            .needs_restart = needs_restart::yes,
            .example = "26214400",
            .visibility = visibility::user,
            .aliases = _aliases,
          };
      }),
      20_MiB,
      {.min = 64_KiB, .max = 100_GiB})
  , data_transforms_per_function_memory_limit(
      *this,
      static_metadata([] {
          static constexpr std::string_view _aliases[] = {
            "wasm_per_function_memory_limit"};
          return base_property::metadata{
            .name = "data_transforms_per_function_memory_limit",
            .desc = "The amount of memory to give an instance of a data "
                    "transform (Wasm) virtual machine. The maximum number of "
                    "functions that can be deployed to a cluster is equal to "
                    "`data_transforms_per_core_memory_reservation` / "
                    "`data_transforms_per_function_memory_limit`.",
            .needs_restart = needs_restart::yes,
            .example = "5242880",
            .visibility = visibility::user,
            .aliases = _aliases,
          };
      }),
      2_MiB,
      // WebAssembly uses 64KiB pages and has a 32bit address space
      {.min = 64_KiB, .max = 4_GiB})
  , data_transforms_runtime_limit_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "data_transforms_runtime_limit_ms",
            .desc
            = "The maximum amount of runtime to start up a data transform, and "
              "the time it takes for a single record to be transformed.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      3s)
  , data_transforms_binary_max_size(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "data_transforms_binary_max_size",
            .desc = "The maximum size for a deployable WebAssembly binary that "
                    "the broker can store.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      10_MiB,
      {.min = 1_MiB, .max = 128_MiB})
  , data_transforms_logging_buffer_capacity_bytes(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "data_transforms_logging_buffer_capacity_bytes",
            .desc = "Buffer capacity for transform logs, per shard. Buffer "
                    "occupancy is calculated as the total size of buffered log "
                    "messages; that is, logs emitted but not yet produced.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      500_KiB,
      {.min = 100_KiB, .max = 2_MiB})
  , data_transforms_logging_flush_interval_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "data_transforms_logging_flush_interval_ms",
            .desc = "Flush interval for transform logs. When a timer expires, "
                    "pending logs are collected and published to the "
                    "`transform_logs` topic.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      500ms)
  , data_transforms_logging_line_max_bytes(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "data_transforms_logging_line_max_bytes",
            .desc = "Transform log lines truncate to this length. Truncation "
                    "occurs after any character escaping.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      1_KiB)
  , data_transforms_read_buffer_memory_percentage(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "data_transforms_read_buffer_memory_percentage",
            .desc = "The percentage of available memory in the transform "
                    "subsystem to use for read buffers.",
            .needs_restart = needs_restart::yes,
            .example = "25",
            .visibility = visibility::tunable,
          };
      }),
      45,
      {.min = 1, .max = 99})
  , data_transforms_write_buffer_memory_percentage(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "data_transforms_write_buffer_memory_percentage",
            .desc = "The percentage of available memory in the transform "
                    "subsystem to use for write buffers.",
            .needs_restart = needs_restart::yes,
            .example = "25",
            .visibility = visibility::tunable,
          };
      }),
      45,
      {.min = 1, .max = 99})
  , topic_memory_per_partition(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "topic_memory_per_partition",
            .desc
            = "Required memory in bytes per partition replica when creating or "
              "altering topics. The total size of the memory pool for "
              "partitions is the total memory available to Redpanda times "
              "topic_partitions_memory_allocation_percent, and then each "
              "partition created requires topic_memory_per_partition bytes "
              "from that pool. If insufficent memory is available, topic "
              "creation or alternation will fail.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      cluster::DEFAULT_TOPIC_MEMORY_PER_PARTITION,
      {
        .min = 1,      // Must be nonzero, it's a divisor
        .max = 100_MiB // Rough 'sanity' limit: a machine with 1GB RAM must be
                       // able to create at least 10 partitions
      })
  , topic_fds_per_partition(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "topic_fds_per_partition",
            .desc = "File descriptors required per partition replica: topic "
                    "creation is prevented if it would result in the ratio of "
                    "file descriptor limit to partition replicas being lower "
                    "than this value.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      5,
      {
        .min = 1,   // At least one FD per partition, required for appender.
        .max = 1000 // A system with 1M ulimit should be allowed to create at
                    // least 1000 partitions
      })
  , topic_partitions_per_shard(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "topic_partitions_per_shard",
            .desc = "Maximum partition replicas per shard: topic creation is "
                    "prevented if it would result in the ratio of partition "
                    "replicas to shards being higher than this value.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      // default value must be synced with
      // scale_parameters.py::DEFAULT_PARTITIONS_PER_SHARD
      5000,
      {
        .min = 16,    // Forbid absurdly small values that would prevent most
                      // practical workloads from running
        .max = 131072 // An upper bound to prevent pathological values, although
                      // systems will most likely hit issues far before reaching
                      // this.  This property is principally intended to be
                      // tuned downward from the default, not upward.
      },
      legacy_default<uint32_t>(7000, legacy_version{9}))
  , topic_partitions_reserve_shard0(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "topic_partitions_reserve_shard0",
            .desc = "Reserved partition slots on shard (CPU core) 0 on each "
                    "node. If this is >= topic_partitions_per_shard, no data "
                    "partitions will be scheduled on shard 0",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      0,
      {
        .min = 0,     // It is not mandatory to reserve any capacity
        .max = 131072 // Same max as topic_partitions_per_shard
      })
  , topic_partitions_memory_allocation_percent(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "topic_partitions_memory_allocation_percent",
            .desc = "Percentage of total memory to reserve for topic "
                    "partitions. See topic_memory_per_partition for details.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      // default value must be synced with
      // scale_parameters.py::DEFAULT_PARTITIONS_MEMORY_ALLOCATION_PERCENT
      10,
      {
        .min = 1,
        .max = 80,
      })
  , partition_manager_shutdown_watchdog_timeout(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "partition_manager_shutdown_watchdog_timeout",
            .desc = "A threshold value to detect partitions which might have "
                    "been stuck while shutting down. After this threshold, a "
                    "watchdog in partition manager will log information about "
                    "partition shutdown not making progress.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      30s)
  , topic_label_aggregation_limit(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "topic_label_aggregation_limit",
            .desc = "When the number of topics exceeds this limit, the topic "
                    "label in generated metrics will be aggregated. If "
                    "`nullopt`, then there is no limit.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      std::nullopt)
  , controller_backend_reconciliation_concurrency(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "controller_backend_reconciliation_concurrency",
            .desc = "Maximum concurrent reconciliation operations the "
                    "controller can run. Higher values can speed up cluster "
                    "state changes but use more resources.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      1024u,
      {.min = 1u, .max = 2048u})
  , admin_api_require_auth(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "admin_api_require_auth",
            .desc = "Whether Admin API clients must provide HTTP basic "
                    "authentication headers.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      false)
  , raft_heartbeat_interval_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "raft_heartbeat_interval_ms",
            .desc = "Number of milliseconds for Raft leader heartbeats.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      std::chrono::milliseconds(150),
      {.min = std::chrono::milliseconds(1)})
  , raft_heartbeat_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "raft_heartbeat_timeout_ms",
            .desc
            = "Raft heartbeat RPC (remote procedure call) timeout. Raft uses a "
              "heartbeat mechanism to maintain leadership authority and to "
              "trigger leader elections. The `raft_heartbeat_interval_ms` is a "
              "periodic heartbeat sent by the partition leader to all "
              "followers to declare its leadership. If a follower does not "
              "receive a heartbeat within the `raft_heartbeat_timeout_ms`, "
              "then it triggers an election to choose a new partition leader.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      3s,
      {.min = std::chrono::milliseconds(1)})
  , raft_heartbeat_disconnect_failures(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "raft_heartbeat_disconnect_failures",
            .desc = "The number of failed heartbeats after which an "
                    "unresponsive TCP connection is forcibly closed. To "
                    "disable forced disconnection, set to 0.",
            .visibility = visibility::tunable,
          };
      }),
      3)
  , raft_max_recovery_memory(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "raft_max_recovery_memory",
            .desc = "Maximum memory that can be used for reads in Raft "
                    "recovery process by default 15% of total memory.",
            .needs_restart = needs_restart::no,
            .example = "41943040",
            .visibility = visibility::tunable,
          };
      }),
      std::nullopt,
      {.min = 32_MiB})
  , raft_enable_lw_heartbeat(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "raft_enable_lw_heartbeat",
            .desc = "Enables Raft optimization of heartbeats.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      true)
  , raft_recovery_concurrency_per_shard(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "raft_recovery_concurrency_per_shard",
            .desc
            = "Number of partitions that may simultaneously recover data to a "
              "particular shard. This number is limited to avoid overwhelming "
              "nodes when they come back online after an outage.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      64,
      {.min = 1, .max = 16384})
  , raft_replica_max_pending_flush_bytes(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "raft_replica_max_pending_flush_bytes",
            .desc
            = "Maximum number of bytes that are not flushed per partition. If "
              "the configured threshold is reached, the log is automatically "
              "flushed even if it has not been explicitly requested.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      256_KiB)
  , raft_flush_timer_interval_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "raft_flush_timer_interval_ms",
            .desc
            = "Interval of checking partition against the "
              "`raft_replica_max_pending_flush_bytes`, deprecated started "
              "24.1, use raft_replica_max_flush_delay_ms instead",
            .visibility = visibility::deprecated,
          };
      }),
      100ms)
  , raft_replica_max_flush_delay_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "raft_replica_max_flush_delay_ms",
            .desc = "Maximum delay between two subsequent flushes. After this "
                    "delay, the log is automatically force flushed.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      100ms,
      [](const auto& v) -> std::optional<ss::sstring> {
          // maximum duration imposed by serde serialization.
          if (v < 1ms || v > serde::max_serializable_ms) {
              return fmt::format(
                "flush delay should be in range: [1, {}]",
                serde::max_serializable_ms);
          }
          return std::nullopt;
      })
  , raft_enable_longest_log_detection(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "raft_enable_longest_log_detection",
            .desc
            = "Enables an additional step in leader election where a candidate "
              "is allowed to wait for all the replies from the broker it "
              "requested votes from. This may introduce a small delay when "
              "recovering from failure, but it prevents truncation if any of "
              "the replicas have more data than the majority.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      true)
  , raft_max_inflight_follower_append_entries_requests_per_shard(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name
            = "raft_max_inflight_follower_append_entries_requests_per_shard",
            .desc = "The maximum number of append entry requests that may be "
                    "sent from raft groups on a Seastar shard to the current "
                    "node and are awaiting a reply. This property replaces "
                    "raft_max_concurrent_append_requests_per_follower.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      1024,
      {.min = 1, .max = 50000})
  , raft_max_buffered_follower_append_entries_bytes_per_shard(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "raft_max_buffered_follower_append_entries_bytes_per_shard",
            .desc
            = "The total size of append entry requests that may be cached per "
              "shard, using Raft buffered protocol. When an entry is cached "
              "the leader can continue serving requests because the ordering "
              "of the cached requests cannot change. When the total size of "
              "cached requests reaches the set limit, back pressure is applied "
              "to throttle producers.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      0,
      {.min = 0, .max = 100_MiB})
  , enable_usage(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "enable_usage",
            .desc = "Enables the usage tracking mechanism, storing windowed "
                    "history of kafka/cloud_storage metrics over time.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      false)
  , usage_num_windows(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "usage_num_windows",
            .desc = "The number of windows to persist in memory and disk.",
            .needs_restart = needs_restart::no,
            .example = "24",
            .visibility = visibility::tunable,
          };
      }),
      24,
      {.min = 2, .max = 86400})
  , usage_window_width_interval_sec(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "usage_window_width_interval_sec",
            .desc = "The width of a usage window, tracking cloud and kafka "
                    "ingress/egress traffic each interval.",
            .needs_restart = needs_restart::no,
            .example = "3600",
            .visibility = visibility::tunable,
          };
      }),
      std::chrono::seconds(3600),
      {.min = std::chrono::seconds(1)})
  , usage_disk_persistance_interval_sec(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "usage_disk_persistance_interval_sec",
            .desc
            = "The interval in which all usage stats are written to disk.",
            .needs_restart = needs_restart::no,
            .example = "300",
            .visibility = visibility::tunable,
          };
      }),
      std::chrono::seconds(60 * 5),
      {.min = std::chrono::seconds(1)})
  , default_num_windows(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "default_num_windows",
            .desc = "Default number of quota tracking windows.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      10,
      {.min = 1})
  , default_window_sec(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "default_window_sec",
            .desc = "Default quota tracking window size in milliseconds.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      std::chrono::milliseconds(1000),
      {.min = std::chrono::milliseconds(1)})
  , quota_manager_gc_sec(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "quota_manager_gc_sec",
            .desc = "Quota manager GC frequency in milliseconds.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      std::chrono::milliseconds(30000))
  , cluster_id(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cluster_id",
            .desc = "Cluster identifier.",
            .needs_restart = needs_restart::no,
            .gets_restored = gets_restored::no,
          };
      }),
      std::nullopt,
      &validate_non_empty_string_opt)
  , disable_metrics(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "disable_metrics",
            .desc = "Disable registering the metrics exposed on the internal "
                    "`/metrics` endpoint.",
          };
      }),
      false)
  , disable_public_metrics(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "disable_public_metrics",
            .desc = "Disable registering the metrics exposed on the "
                    "`/public_metrics` endpoint.",
          };
      }),
      false)
  , aggregate_metrics(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "aggregate_metrics",
            .desc = "Enable aggregation of metrics returned by the `/metrics` "
                    "endpoint. Aggregation can simplify monitoring by "
                    "providing summarized data instead of raw, per-instance "
                    "metrics. Metric aggregation is performed by summing the "
                    "values of samples by labels and is done when it makes "
                    "sense by the shard and/or partition labels.",
            .needs_restart = needs_restart::no,
          };
      }),
      false)
  , enable_consumer_group_metrics(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "enable_consumer_group_metrics",
            .desc = "List of enabled consumer group metrics. Accepted Values: "
                    "`group`, `partition`, `consumer_lag`",
            .needs_restart = needs_restart::no,
          };
      }),
      std::vector<ss::sstring>{"group", "partition"},
      validate_consumer_group_metrics)
  , consumer_group_lag_collection_interval(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "consumer_group_lag_collection_interval_sec",
            .desc = "How often Redpanda runs the collection loop when "
                    "`enable_consumer_group_metrics` is set to `consumer_lag`. "
                    "Updates will not be more frequent than "
                    "`health_monitor_max_metadata_age`.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      60s)
  , group_min_session_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "group_min_session_timeout_ms",
            .desc = "The minimum allowed session timeout for registered "
                    "consumers. Shorter timeouts result in quicker failure "
                    "detection at the cost of more frequent consumer "
                    "heartbeating, which can overwhelm broker resources.",
            .needs_restart = needs_restart::no,
          };
      }),
      6000ms)
  , group_max_session_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "group_max_session_timeout_ms",
            .desc = "The maximum allowed session timeout for registered "
                    "consumers. Longer timeouts give consumers more time to "
                    "process messages in between heartbeats at the cost of a "
                    "longer time to detect failures.",
            .needs_restart = needs_restart::no,
          };
      }),
      300s)
  , group_initial_rebalance_delay(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "group_initial_rebalance_delay",
            .desc
            = "Delay added to the rebalance phase to wait for new members.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      3s)
  , group_new_member_join_timeout(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "group_new_member_join_timeout",
            .desc = "Timeout for new member joins.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      30'000ms)
  , group_offset_retention_sec(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "group_offset_retention_sec",
            .desc = "Consumer group offset retention seconds. To disable "
                    "offset retention, set this to null.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      24h * 7)
  , group_offset_retention_check_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "group_offset_retention_check_ms",
            .desc = "Frequency rate at which the system should check for "
                    "expired group offsets.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      10min)
  , legacy_group_offset_retention_enabled(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "legacy_group_offset_retention_enabled",
            .desc = "Group offset retention is enabled by default starting in "
                    "Redpanda version 23.1. To enable offset retention after "
                    "upgrading from an older version, set this option to true.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , metadata_dissemination_interval_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "metadata_dissemination_interval_ms",
            .desc = "Interval for metadata dissemination batching.",
            .example = "5000",
            .visibility = visibility::tunable,
          };
      }),
      3'000ms)
  , metadata_dissemination_retry_delay_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "metadata_dissemination_retry_delay_ms",
            .desc = "Delay before retrying a topic lookup in a shard or other "
                    "meta tables.",
            .visibility = visibility::tunable,
          };
      }),
      0'500ms)
  , metadata_dissemination_retries(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "metadata_dissemination_retries",
            .desc
            = "Number of attempts to look up a topic's metadata-like shard "
              "before a request fails. This configuration controls the number "
              "of retries that request handlers perform when internal topic "
              "metadata (for topics like tx, consumer offsets, etc) is "
              "missing. These topics are usually created on demand when users "
              "try to use the cluster for the first time and it may take some "
              "time for the creation to happen and the metadata to propagate "
              "to all the brokers (particularly the broker handling the "
              "request). In the mean time Redpanda waits and retry. This "
              "configuration controls the number retries.",
            .visibility = visibility::tunable,
          };
      }),
      30)
  , tx_timeout_delay_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "tx_timeout_delay_ms",
            .desc = "Delay before scheduling the next check for timed out "
                    "transactions.",
            .visibility = visibility::user,
          };
      }),
      1000ms)
  , fetch_reads_debounce_timeout(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "fetch_reads_debounce_timeout",
            .desc = "Time to wait for the next read in fetch requests when the "
                    "requested minimum bytes was not reached.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      1ms)
  , kafka_fetch_request_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_fetch_request_timeout_ms",
            .desc = "Broker-side target for the duration of a single fetch "
                    "request. The broker will try to complete fetches within "
                    "the specified duration, even if it means returning less "
                    "bytes in the fetch than are available.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      5s)
  , enable_listoffsets_historical_leader_epoch(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "enable_listoffsets_historical_leader_epoch",
            .desc
            = "When enabled, the Kafka ListOffsets API returns the historical "
              "(record-time) leader epoch instead of the current leader epoch. "
              "Intended as a one-way opt-in: disabling after it has been "
              "enabled regresses to the original bug. Gated as a development "
              "feature: not all response paths are fixed yet (CORE-12505), so "
              "enabling this property produces internally inconsistent epoch "
              "values across paths and must not be enabled in production.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , fetch_read_strategy(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "fetch_read_strategy",
            .desc
            = "The strategy used to fulfill fetch requests. * `polling`: If "
              "`fetch_reads_debounce_timeout` is set to its default value, "
              "then this acts exactly like `non_polling`; otherwise, it acts "
              "like `non_polling_with_debounce` (deprecated). * `non_polling`: "
              "The backend is signaled when a partition has new data, so "
              "Redpanda does not need to repeatedly read from every partition "
              "in the fetch. Redpanda Data recommends using this value for "
              "most workloads, because it can improve fetch latency and CPU "
              "utilization. * `non_polling_with_debounce`: This option behaves "
              "like `non_polling`, but it includes a debounce mechanism with a "
              "fixed delay specified by `fetch_reads_debounce_timeout` at the "
              "start of each fetch. By introducing this delay, Redpanda can "
              "accumulate more data before processing, leading to fewer fetch "
              "operations and returning larger amounts of data. Enabling this "
              "option reduces reactor utilization, but it may also increase "
              "end-to-end latency.",
            .needs_restart = needs_restart::no,
            .example = model::fetch_read_strategy_to_string(
              model::fetch_read_strategy::non_polling),
            .visibility = visibility::tunable,
          };
      }),
      model::fetch_read_strategy::non_polling,
      {
        model::fetch_read_strategy::polling,
        model::fetch_read_strategy::non_polling,
        model::fetch_read_strategy::non_polling_with_debounce,
        model::fetch_read_strategy::non_polling_with_pid,
      })
  , fetch_max_read_concurrency(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "fetch_max_read_concurrency",
            .desc = "The maximum number of concurrent partition reads per "
                    "fetch request on each shard. Setting this higher than the "
                    "default can lead to partition starvation and unneeded "
                    "memory usage.",
            .needs_restart = needs_restart::no,
            .example = "1",
            .visibility = visibility::tunable,
          };
      }),
      1,
      {.min = 1, .max = 100})
  , fetch_pid_p_coeff(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "fetch_pid_p_coeff",
            .desc = "Proportional coefficient for fetch PID controller.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      100.0,
      {.min = 0.0, .max = std::numeric_limits<double>::max()})
  , fetch_pid_i_coeff(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "fetch_pid_i_coeff",
            .desc = "Integral coefficient for fetch PID controller.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      0.01,
      {.min = 0.0, .max = std::numeric_limits<double>::max()})
  , fetch_pid_d_coeff(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "fetch_pid_d_coeff",
            .desc = "Derivative coefficient for fetch PID controller.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      0.0,
      {.min = 0.0, .max = std::numeric_limits<double>::max()})
  , fetch_pid_target_utilization_fraction(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "fetch_pid_target_utilization_fraction",
            .desc = "A fraction, between 0 and 1, for the target reactor "
                    "utilization of the fetch scheduling group.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      0.2,
      {.min = 0.01, .max = 1.0})
  , fetch_pid_max_debounce_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "fetch_pid_max_debounce_ms",
            .desc = "The maximum debounce time the fetch PID controller will "
                    "apply, in milliseconds.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      100ms)
  , log_cleanup_policy(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "log_cleanup_policy",
            .desc = "Default cleanup policy for topic logs. The topic property "
                    "`cleanup.policy` overrides the value of "
                    "`log_cleanup_policy` at the topic level.",
            .needs_restart = needs_restart::no,
            .example = "compact,delete",
            .visibility = visibility::user,
          };
      }),
      model::cleanup_policy_bitflags::deletion)
  , log_message_timestamp_type(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "log_message_timestamp_type",
            .desc
            = "Default timestamp type for topic messages (CreateTime or "
              "LogAppendTime). The topic property `message.timestamp.type` "
              "overrides the value of `log_message_timestamp_type` at the "
              "topic level.",
            .needs_restart = needs_restart::no,
            .example = "LogAppendTime",
            .visibility = visibility::user,
          };
      }),
      model::timestamp_type::create_time,
      {model::timestamp_type::create_time, model::timestamp_type::append_time})
  , log_message_timestamp_before_max_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "log_message_timestamp_before_max_ms",
            .desc
            = "The maximum allowable timestamp difference between the broker's "
              "timestamp and a record's timestamp. For topics with "
              "`message.timestamp.type` set to `CreateTime`, Redpanda rejects "
              "records that have timestamps earlier than the broker timestamp "
              "and exceed this difference. Redpanda ignores this property for "
              "topics with `message.timestamp.type` set to `AppendTime`. The "
              "topic property `message.timestamp.before.max.ms` overrides the "
              "value of `log_message_timestamp_before_max_ms` at the topic "
              "level.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      serde::max_serializable_ms,
      {.min = 0ms, .max = serde::max_serializable_ms})
  , log_message_timestamp_after_max_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "log_message_timestamp_after_max_ms",
            .desc
            = "The maximum allowable timestamp difference between the broker's "
              "timestamp and a record's timestamp. For topics with "
              "`message.timestamp.type` set to `CreateTime`, Redpanda rejects "
              "records that have timestamps later than the broker timestamp "
              "and exceed this difference. Redpanda ignores this property for "
              "topics with `message.timestamp.type` set to `AppendTime`. The "
              "topic property `message.timestamp.after.max.ms` overrides the "
              "value of `log_message_timestamp_after_max_ms` at the topic "
              "level.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      1h,
      {.min = 0ms, .max = serde::max_serializable_ms})
  , kafka_produce_batch_validation(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_produce_batch_validation",
            .desc
            = "Controls the level of validation performed on batches produced "
              "to Redpanda. When set to `legacy`, there is minimal validation "
              "performed on the produce path. When set to `relaxed`, full "
              "validation is performed on uncompressed batches and on "
              "compressed batches with the `max_timestamp` value left unset. "
              "When set to `strict`, full validation of uncompressed and "
              "compressed batches is performed. This should be the default in "
              "environments where producing clients are not trusted.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      model::kafka_batch_validation_mode::relaxed,
      {model::kafka_batch_validation_mode::legacy,
       model::kafka_batch_validation_mode::relaxed,
       model::kafka_batch_validation_mode::strict})
  , log_compression_type(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "log_compression_type",
            .desc = "Default topic compression type. The topic property "
                    "`compression.type` overrides the value of "
                    "`log_compression_type` at the topic level.",
            .needs_restart = needs_restart::no,
            .example = "snappy",
            .visibility = visibility::user,
          };
      }),
      model::compression::producer,
      {model::compression::none,
       model::compression::gzip,
       model::compression::snappy,
       model::compression::lz4,
       model::compression::zstd,
       model::compression::producer})
  , fetch_max_bytes(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "fetch_max_bytes",
            .desc = "Maximum number of bytes returned in a fetch request.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      55_MiB)
  , use_fetch_scheduler_group(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "use_fetch_scheduler_group",
            .desc = "Use a separate scheduler group for fetch processing.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      true)
  , use_produce_scheduler_group(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "use_produce_scheduler_group",
            .desc = "Use a separate scheduler group for kafka produce requests "
                    "processing.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      true)
  , use_kafka_handler_scheduler_group(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "use_kafka_handler_scheduler_group",
            .desc = "Use separate scheduler group to handle parsing Kafka "
                    "protocol requests",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      true)
  , kafka_handler_latency_all(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_handler_latency_all",
            .desc = "Enable latency histograms for all Kafka API handlers. "
                    "When disabled, only important handlers (produce, fetch, "
                    "metadata, api_versions, offset_commit) have latency "
                    "histograms.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , kafka_tcp_keepalive_idle_timeout_seconds(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_tcp_keepalive_timeout",
            .desc = "TCP keepalive idle timeout in seconds for Kafka "
                    "connections. This describes the timeout between TCP "
                    "keepalive probes that the remote site successfully "
                    "acknowledged. Refers to the TCP_KEEPIDLE socket option. "
                    "When changed, applies to new connections only.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      120s)
  , kafka_tcp_keepalive_probe_interval_seconds(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_tcp_keepalive_probe_interval_seconds",
            .desc
            = "TCP keepalive probe interval in seconds for Kafka connections. "
              "This describes the timeout between unacknowledged TCP "
              "keepalives. Refers to the TCP_KEEPINTVL socket option. When "
              "changed, applies to new connections only.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      60s)
  , kafka_tcp_keepalive_probes(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_tcp_keepalive_probes",
            .desc = "TCP keepalive unacknowledged probes until the connection "
                    "is considered dead for Kafka connections. Refers to the "
                    "TCP_KEEPCNT socket option. When changed, applies to new "
                    "connections only.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      3)
  , kafka_connection_rate_limit(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_connection_rate_limit",
            .desc = "Maximum connections per second for one core. If `null` "
                    "(the default), then the number of connections per second "
                    "is unlimited.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      std::nullopt,
      {.min = 1})
  , kafka_connection_rate_limit_overrides(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_connection_rate_limit_overrides",
            .desc = "Overrides the maximum connections per second for one core "
                    "for the specified IP addresses (for example, "
                    "`['127.0.0.1:90', '50.20.1.1:40']`)",
            .needs_restart = needs_restart::no,
            .example = R"(['127.0.0.1:90', '50.20.1.1:40'])",
            .visibility = visibility::user,
          };
      }),
      {},
      validate_connection_rate)
  , transactional_id_expiration_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "transactional_id_expiration_ms",
            .desc
            = "Expiration time of producer IDs. Measured starting from the "
              "time of the last write until now for a given ID. Producer IDs "
              "are automatically removed from memory when they expire, which "
              "helps manage memory usage. However, this natural cleanup may "
              "not be sufficient for workloads with high producer churn rates. "
              "For applications with long-running transactions, ensure this "
              "value accommodates your typical transaction lifetime to avoid "
              "premature producer ID expiration.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      10080min)
  , max_concurrent_producer_ids(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "max_concurrent_producer_ids",
            .desc
            = "Maximum number of active producer sessions per shard. Each "
              "shard tracks producer IDs using an LRU (Least Recently Used) "
              "eviction policy. When the configured limit is exceeded, the "
              "least recently used producer IDs are evicted from the cache.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      100000,
      {.min = 1})
  , max_transactions_per_coordinator(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "max_transactions_per_coordinator",
            .desc
            = "Specifies the maximum number of active transaction sessions per "
              "coordinator. When the threshold is passed Redpanda terminates "
              "old sessions. When an idle producer corresponding to the "
              "terminated session wakes up and produces, it leads to its "
              "batches being rejected with invalid producer epoch or "
              "invalid_producer_id_mapping error (depends on the transaction "
              "execution phase).",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      10000,
      {.min = 1})
  , enable_idempotence(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "enable_idempotence",
            .desc = "Enable idempotent producers.",
            .visibility = visibility::user,
          };
      }),
      true)
  , enable_transactions(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "enable_transactions",
            .desc = "Enable transactions (atomic writes).",
            .visibility = visibility::user,
          };
      }),
      true)
  , abort_index_segment_size(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "abort_index_segment_size",
            .desc = "Capacity (in number of txns) of an abort index segment. "
                    "Each partition tracks the aborted transaction offset "
                    "ranges to help service client requests.If the number "
                    "transactions increase beyond this threshold, they are "
                    "flushed to disk to easy memory pressure.Then they're "
                    "loaded on demand. This configuration controls the maximum "
                    "number of aborted transactions before they are flushed to "
                    "disk.",
            .visibility = visibility::tunable,
          };
      }),
      50000)
  , log_retention_ms(
      *this,
      static_metadata([] {
          static constexpr std::string_view _aliases[] = {
            "delete_retention_ms"};
          return base_property::metadata{
            .name = "log_retention_ms",
            .desc
            = "The amount of time to keep a log file before deleting it (in "
              "milliseconds). If set to `-1`, no time limit is applied. This "
              "is a cluster-wide default when a topic does not set or disable "
              "`retention.ms`.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
            .aliases = _aliases,
          };
      }),
      7 * 24h)
  , log_compaction_interval_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "log_compaction_interval_ms",
            .desc = "How often to trigger background compaction.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      10s)
  , log_compaction_max_priority_wait_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "log_compaction_max_priority_wait_ms",
            .desc = "Maximum time a priority partition (for example, "
                    "__consumer_offsets) can wait for compaction before "
                    "preempting regular compaction.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      60min)
  , tombstone_retention_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "tombstone_retention_ms",
            .desc = "The retention time for tombstone records and transaction "
                    "markers in a compacted topic.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      std::nullopt,
      validate_tombstone_retention_ms)
  , min_cleanable_dirty_ratio(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "min_cleanable_dirty_ratio",
            .desc = "The minimum ratio between the number of bytes in "
                    "\"dirty\" segments and the total number of bytes in "
                    "closed segments that must be reached before a partition's "
                    "log is eligible for compaction in a compact topic. The "
                    "topic property `min.cleanable.dirty.ratio` overrides the "
                    "value of `min_cleanable_dirty_ratio` at the topic level.",
            .needs_restart = needs_restart::no,
            .example = "0.2",
            .visibility = visibility::user,
          };
      }),
      0.2,
      {.min = 0.0, .max = 1.0})
  , min_compaction_lag_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "min_compaction_lag_ms",
            .desc = "For a compacted topic, the minimum time a message remains "
                    "uncompacted in the log. The topic property "
                    "`min.compaction.lag.ms` overrides this property.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      0ms,
      [](const auto& v) -> std::optional<ss::sstring> {
          // Maximum duration imposed by serde serialization.
          if (v < 0ms || v > serde::max_serializable_ms) {
              return fmt::format(
                "min compaction lag should be in range: [0, {}]",
                serde::max_serializable_ms);
          }
          return std::nullopt;
      })
  , max_compaction_lag_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "max_compaction_lag_ms",
            .desc = "For a compacted topic, the maximum time a message remains "
                    "ineligible for compaction. The topic property "
                    "`max.compaction.lag.ms` overrides this property.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      serde::max_serializable_ms,
      [](const auto& v) -> std::optional<ss::sstring> {
          // Maximum duration imposed by serde serialization.
          if (v < 1ms || v > serde::max_serializable_ms) {
              return fmt::format(
                "max compaction lag should be in range: [1, {}]",
                serde::max_serializable_ms);
          }
          return std::nullopt;
      })
  , log_disable_housekeeping_for_tests(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "log_disable_housekeeping_for_tests",
            .desc = "Disables the housekeeping loop for local storage. This "
                    "property is used to simplify testing, and should not be "
                    "set in production.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , log_compaction_use_sliding_window(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "log_compaction_use_sliding_window",
            .desc = "Use sliding window compaction.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      true)
  , log_compaction_pause_use_sliding_window(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "log_compaction_pause_use_sliding_window",
            .desc = "Pause use of sliding window compaction. This should only "
                    "be toggled to `true` when it is desired to force adjacent "
                    "segment compaction. The memory reserved by "
                    "`storage_compaction_key_map_memory` is not freed when "
                    "this is set to `true`.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , log_compaction_merge_max_segments_per_range(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "log_compaction_merge_max_segments_per_range",
            .desc = "The maximum number of segments that can be combined into "
                    "a single segment during an adjacent merge operation. If "
                    "`null` (the default value), no maximum is imposed on the "
                    "number of segments that can be combined at once. A value "
                    "below 2 effectively disables adjacent merge compaction.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      std::nullopt)
  , log_compaction_merge_max_ranges(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "log_compaction_merge_max_ranges",
            .desc = "The maximum number of ranges of segments that can be "
                    "processed in a single round of adjacent segment "
                    "compaction. If `null` (the default value), no maximum is "
                    "imposed on the number of ranges that can be processed at "
                    "once. A value below 1 effectively disables adjacent merge "
                    "compaction.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      std::nullopt)
  , log_compaction_tx_batch_removal_enabled(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "log_compaction_tx_batch_removal_enabled",
            .desc
            = "Enables removal of transactional control batches during "
              "compaction. These batches are removed according to a topic's "
              "configured delete.retention.ms, and only if the topic's "
              "cleanup.policy allows compaction.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      true,
      property<bool>::noop_validator,
      legacy_default<bool>(false, legacy_version{17}))
  , retention_bytes(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "retention_bytes",
            .desc = "Default maximum number of bytes per partition on disk "
                    "before triggering deletion of the oldest messages. If "
                    "`null` (the default value), no limit is applied. The "
                    "topic property `retention.bytes` overrides the value of "
                    "`retention_bytes` at the topic level.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      std::nullopt)
  , group_topic_partitions(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "group_topic_partitions",
            .desc
            = "Number of partitions in the internal group membership topic.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      16)
  , default_topic_replication(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "default_topic_replications",
            .desc = "Default replication factor for new topics.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      1,
      {.min = 1, .oddeven = odd_even_constraint::odd})
  , minimum_topic_replication(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "minimum_topic_replications",
            .desc
            = "Minimum allowable replication factor for topics in this "
              "cluster. The set value must be positive, odd, and equal to or "
              "less than the number of available brokers. Changing this "
              "parameter only restricts newly-created topics. Redpanda returns "
              "an `INVALID_REPLICATION_FACTOR` error on any attempt to create "
              "a topic with a replication factor less than this property. If "
              "you change the `minimum_topic_replications` setting, the "
              "replication factor of existing topics remains unchanged. "
              "However, Redpanda will log a warning on start-up with a list of "
              "any topics that have fewer replicas than this minimum. For "
              "example, you might see a message such as `Topic X has a "
              "replication factor less than specified minimum: 1 < 3`.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      1,
      {.min = 1, .oddeven = odd_even_constraint::odd})
  , transaction_coordinator_partitions(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "transaction_coordinator_partitions",
            .desc = "Number of partitions for transactions coordinator.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      50)
  , transaction_coordinator_cleanup_policy(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "transaction_coordinator_cleanup_policy",
            .desc = "Cleanup policy for a transaction coordinator topic. "
                    "Accepted Values: `compact`, `delete`, "
                    "`[\"compact\",\"delete\"]`, `none`",
            .needs_restart = needs_restart::no,
            .example = "compact,delete",
            .visibility = visibility::user,
          };
      }),
      model::cleanup_policy_bitflags::deletion)
  , transaction_coordinator_delete_retention_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "transaction_coordinator_delete_retention_ms",
            .desc = "Delete segments older than this age. To ensure "
                    "transaction state is retained as long as the "
                    "longest-running transaction, make sure this is no less "
                    "than `transactional_id_expiration_ms`.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      10080min)
  , transaction_coordinator_log_segment_size(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "transaction_coordinator_log_segment_size",
            .desc = "The size (in bytes) each log segment should be.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      1_GiB)
  , abort_timed_out_transactions_interval_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "abort_timed_out_transactions_interval_ms",
            .desc
            = "Interval, in milliseconds, at which Redpanda looks for inactive "
              "transactions and aborts them.",
            .visibility = visibility::tunable,
          };
      }),
      10s)
  , transaction_max_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "transaction_max_timeout_ms",
            .desc
            = "The maximum allowed timeout for transactions. If a "
              "client-requested transaction timeout exceeds this "
              "configuration, the broker returns an error during transactional "
              "producer initialization. This guardrail prevents hanging "
              "transactions from blocking consumer progress.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      15min)
  , tx_log_stats_interval_s(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "tx_log_stats_interval_s",
            .desc = "How often to log per partition tx stats, works only with "
                    "debug logging enabled.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::deprecated,
          };
      }),
      10s)
  , default_topic_partitions(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "default_topic_partitions",
            .desc = "Default number of partitions per topic.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      1)
  , disable_batch_cache(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "disable_batch_cache",
            .desc = "Disable batch cache in log manager.",
            .visibility = visibility::tunable,
          };
      }),
      false)
  , raft_election_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "election_timeout_ms",
            .desc = "Election timeout expressed in milliseconds.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      1'500ms)
  , kafka_group_recovery_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_group_recovery_timeout_ms",
            .desc = "Kafka group recovery timeout.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      30'000ms)
  , replicate_append_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "replicate_append_timeout_ms",
            .desc = "Timeout for append entry requests issued while "
                    "replicating entries.",
            .visibility = visibility::tunable,
          };
      }),
      3s)
  , raft_replicate_batch_window_size(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "raft_replicate_batch_window_size",
            .desc = "Maximum size of requests cached for replication.",
            .visibility = visibility::tunable,
          };
      }),
      1_MiB)
  , raft_learner_recovery_rate(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "raft_learner_recovery_rate",
            .desc
            = "Raft learner recovery rate limit. Throttles the rate of data "
              "communicated to nodes (learners) that need to catch up to "
              "leaders. This rate limit is placed on a node sending data to a "
              "recovering node. Each sending node is limited to this rate. The "
              "recovering node accepts data as fast as possible according to "
              "the combined limits of all healthy nodes in the cluster. For "
              "example, if two nodes are sending data to the recovering node, "
              "and `raft_learner_recovery_rate` is 100 MB/sec, then the "
              "recovering node will recover at a rate of 200 MB/sec.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      100_MiB)
  , raft_recovery_throttle_disable_dynamic_mode(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "raft_recovery_throttle_disable_dynamic_mode",
            .desc = "Disables cross shard sharing used to throttle recovery "
                    "traffic. Should only be used to debug unexpected "
                    "problems.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , raft_smp_max_non_local_requests(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "raft_smp_max_non_local_requests",
            .desc
            = "Maximum number of Cross-core(Inter-shard communication) "
              "requests pending in Raft seastar::smp group. For details, refer "
              "to the `seastar::smp_service_group` documentation).",
            .visibility = visibility::tunable,
          };
      }),
      std::nullopt)
  , write_caching_default(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "write_caching_default",
            .desc = "The default write caching mode to apply to user topics. "
                    "Write caching acknowledges a message as soon as it is "
                    "received and acknowledged on a majority of brokers, "
                    "without waiting for it to be written to disk. With "
                    "`acks=all`, this provides lower latency while still "
                    "ensuring that a majority of brokers acknowledge the "
                    "write. Fsyncs follow "
                    "`raft_replica_max_pending_flush_bytes` and "
                    "`raft_replica_max_flush_delay_ms`, whichever is reached "
                    "first. The `write_caching_default` cluster property can "
                    "be overridden with the `write.caching` topic property. "
                    "Accepted values: * `true` * `false` * `disabled`: This "
                    "takes precedence over topic overrides and disables write "
                    "caching for the entire cluster.",
            .needs_restart = needs_restart::no,
            .example = "true",
            .visibility = visibility::user,
          };
      }),
      model::write_caching_mode::default_false,
      {model::write_caching_mode::default_true,
       model::write_caching_mode::default_false,
       model::write_caching_mode::disabled})
  , reclaim_min_size(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "reclaim_min_size",
            .desc = "Minimum batch cache reclaim size.",
            .visibility = visibility::tunable,
          };
      }),
      128_KiB)
  , reclaim_max_size(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "reclaim_max_size",
            .desc = "Maximum batch cache reclaim size.",
            .visibility = visibility::tunable,
          };
      }),
      4_MiB)
  , reclaim_growth_window(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "reclaim_growth_window",
            .desc = "Starting from the last point in time when memory was "
                    "reclaimed from the batch cache, this is the duration "
                    "during which the amount of memory to reclaim grows at a "
                    "significant rate, based on heuristics about the amount of "
                    "available memory.",
            .visibility = visibility::tunable,
          };
      }),
      3'000ms)
  , reclaim_stable_window(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "reclaim_stable_window",
            .desc
            = "If the duration since the last time memory was reclaimed is "
              "longer than the amount of time specified in this property, the "
              "memory usage of the batch cache is considered stable, so only "
              "the minimum size (`reclaim_min_size`) is set to be reclaimed.",
            .visibility = visibility::tunable,
          };
      }),
      10'000ms)
  , reclaim_batch_cache_min_free(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "reclaim_batch_cache_min_free",
            .desc = "Minimum amount of free memory maintained by the batch "
                    "cache background reclaimer.",
            .visibility = visibility::tunable,
          };
      }),
      64_MiB)
  , auto_create_topics_enabled(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "auto_create_topics_enabled",
            .desc = "Allow automatic topic creation. To prevent excess topics, "
                    "this property is not supported on Redpanda Cloud BYOC and "
                    "Dedicated clusters. You should explicitly manage topic "
                    "creation for these Redpanda Cloud clusters. If you "
                    "produce to a topic that doesn't exist, the topic will be "
                    "created with defaults if this property is enabled.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      false)
  , enable_pid_file(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "enable_pid_file",
            .desc = "Enable PID file. You should not need to change.",
            .visibility = visibility::tunable,
          };
      }),
      true)
  , kvstore_flush_interval(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kvstore_flush_interval",
            .desc = "Key-value store flush interval (in milliseconds).",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      std::chrono::milliseconds(10))
  , kvstore_max_segment_size(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kvstore_max_segment_size",
            .desc = "Key-value maximum segment size (in bytes).",
            .visibility = visibility::tunable,
          };
      }),
      16_MiB)
  , max_kafka_throttle_delay_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "max_kafka_throttle_delay_ms",
            .desc = "Fail-safe maximum throttle delay on Kafka requests.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      30'000ms)
  , kafka_max_bytes_per_fetch(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_max_bytes_per_fetch",
            .desc
            = "Limit fetch responses to this many bytes, even if the total of "
              "partition bytes limits is higher.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      64_MiB)
  , raft_io_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "raft_io_timeout_ms",
            .desc = "Raft I/O timeout.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      10'000ms)
  , join_retry_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "join_retry_timeout_ms",
            .desc = "Time between cluster join retries in milliseconds.",
            .visibility = visibility::tunable,
          };
      }),
      5s)
  , raft_timeout_now_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "raft_timeout_now_timeout_ms",
            .desc
            = "Timeout for Raft's timeout_now RPC. This RPC is used to force a "
              "follower to dispatch a round of votes immediately.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      1s)
  , raft_transfer_leader_recovery_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "raft_transfer_leader_recovery_timeout_ms",
            .desc = "Follower recovery timeout waiting period when "
                    "transferring leadership.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      10s)
  , release_cache_on_segment_roll(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "release_cache_on_segment_roll",
            .desc = "Flag for specifying whether or not to release cache when "
                    "a full segment is rolled.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , segment_appender_flush_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "segment_appender_flush_timeout_ms",
            .desc = "Maximum delay until buffered data is written.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      std::chrono::milliseconds(1s))
  , fetch_session_eviction_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "fetch_session_eviction_timeout_ms",
            .desc = "Time duration after which the inactive fetch session is "
                    "removed from the fetch session cache. Fetch sessions are "
                    "used to implement the incremental fetch requests where a "
                    "consumer does not send all requested partitions to the "
                    "server but the server tracks them for the consumer.",
            .visibility = visibility::tunable,
          };
      }),
      60s)
  , append_chunk_size(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "append_chunk_size",
            .desc = "Size of direct write operations to disk in bytes. A "
                    "larger chunk size can improve performance for write-heavy "
                    "workloads, but increase latency for these writes as more "
                    "data is collected before each write operation. A smaller "
                    "chunk size can decrease write latency, but potentially "
                    "increase the number of disk I/O operations.",
            .example = "32768",
            .visibility = visibility::tunable,
          };
      }),
      16_KiB,
      {.min = 4096, .max = 32_MiB, .align = 4096})
  , storage_read_buffer_size(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "storage_read_buffer_size",
            .desc = "Size of each read buffer (one per in-flight read, per log "
                    "segment).",
            .needs_restart = needs_restart::no,
            .example = "31768",
            .visibility = visibility::tunable,
          };
      }),
      128_KiB)
  , storage_read_readahead_count(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "storage_read_readahead_count",
            .desc = "How many additional reads to issue ahead of current read "
                    "location.",
            .needs_restart = needs_restart::no,
            .example = "1",
            .visibility = visibility::tunable,
          };
      }),
      1)
  , segment_fallocation_step(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "segment_fallocation_step",
            .desc = "Size for segments fallocation.",
            .needs_restart = needs_restart::no,
            .example = "32768",
            .visibility = visibility::tunable,
          };
      }),
      32_MiB,
      storage::validate_fallocation_step)
  , storage_target_replay_bytes(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "storage_target_replay_bytes",
            .desc = "Target bytes to replay from disk on startup after clean "
                    "shutdown: controls frequency of snapshots and "
                    "checkpoints.",
            .needs_restart = needs_restart::no,
            .example = "2147483648",
            .visibility = visibility::tunable,
          };
      }),
      10_GiB,
      {.min = 128_MiB, .max = 1_TiB})
  , storage_max_concurrent_replay(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "storage_max_concurrent_replay",
            .desc = "Maximum number of partitions' logs that will be replayed "
                    "concurrently at startup, or flushed concurrently on "
                    "shutdown.",
            .needs_restart = needs_restart::no,
            .example = "2048",
            .visibility = visibility::tunable,
          };
      }),
      1024,
      {.min = 128})
  , storage_compaction_index_memory(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "storage_compaction_index_memory",
            .desc = "Maximum number of bytes that may be used on each shard by "
                    "compaction index writers.",
            .needs_restart = needs_restart::no,
            .example = "1073741824",
            .visibility = visibility::tunable,
          };
      }),
      128_MiB,
      {.min = 16_MiB, .max = 100_GiB})
  , storage_compaction_key_map_memory(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "storage_compaction_key_map_memory",
            .desc = "Maximum number of bytes that may be used on each shard by "
                    "compaction key-offset maps. Only applies when "
                    "`log_compaction_use_sliding_window` is set to `true`.",
            .needs_restart = needs_restart::yes,
            .example = "1073741824",
            .visibility = visibility::tunable,
          };
      }),
      128_MiB,
      {.min = 16_MiB, .max = 100_GiB})
  , storage_compaction_key_map_memory_limit_percent(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "storage_compaction_key_map_memory_limit_percent",
            .desc
            = "Limit on `storage_compaction_key_map_memory`, expressed as a "
              "percentage of memory per shard, that bounds the amount of "
              "memory used by compaction key-offset maps. Memory per shard is "
              "computed after `data_transforms_per_core_memory_reservation`, "
              "and only applies when `log_compaction_use_sliding_window` is "
              "set to `true`.",
            .needs_restart = needs_restart::yes,
            .example = "12.0",
            .visibility = visibility::tunable,
          };
      }),
      12.0,
      {.min = 1.0, .max = 100.0})
  , max_compacted_log_segment_size(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "max_compacted_log_segment_size",
            .desc = "Maximum compacted segment size after consolidation.",
            .needs_restart = needs_restart::no,
            .example = "10737418240",
            .visibility = visibility::tunable,
          };
      }),
      512_MiB)
  , storage_ignore_timestamps_in_future_sec(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "storage_ignore_timestamps_in_future_sec",
            .desc
            = "The maximum number of seconds that a record's timestamp can be "
              "ahead of a Redpanda broker's clock and still be used when "
              "deciding whether to clean up the record for data retention. "
              "This property makes possible the timely cleanup of records from "
              "clients with clocks that are drastically unsynchronized "
              "relative to Redpanda. When determining whether to clean up a "
              "record with timestamp more than "
              "`storage_ignore_timestamps_in_future_sec` seconds ahead of the "
              "broker, Redpanda ignores the record's timestamp and instead "
              "uses a valid timestamp of another record in the same segment, "
              "or (if another record's valid timestamp is unavailable) the "
              "timestamp of when the segment file was last modified (mtime). "
              "By default, `storage_ignore_timestamps_in_future_sec` is "
              "disabled (null). To figure out whether to set "
              "`storage_ignore_timestamps_in_future_sec` for your system: . "
              "Look for logs with segments that are unexpectedly large and not "
              "being cleaned up. . In the logs, search for records with "
              "unsynchronized timestamps that are further into the future than "
              "tolerable by your data retention and storage settings. For "
              "example, timestamps 60 seconds or more into the future can be "
              "considered to be too unsynchronized. . If you find "
              "unsynchronized timestamps throughout your logs, determine the "
              "number of seconds that the timestamps are ahead of their actual "
              "time, and set `storage_ignore_timestamps_in_future_sec` to that "
              "value so data retention can proceed. . If you only find "
              "unsynchronized timestamps that are the result of transient "
              "behavior, you can disable "
              "`storage_ignore_timestamps_in_future_sec`.",
            .needs_restart = needs_restart::no,
            .example = "3600",
            .visibility = visibility::tunable,
          };
      }),
      std::nullopt)
  , storage_ignore_cstore_hints(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "storage_ignore_cstore_hints",
            .desc = "When set, cstore hints are ignored and not used for data "
                    "access (but are otherwise generated).",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , storage_reserve_min_segments(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "storage_reserve_min_segments",
            .desc = "The number of segments per partition that the system will "
                    "attempt to reserve disk capacity for. For example, if the "
                    "maximum segment size is configured to be 100 MB, and the "
                    "value of this option is 2, then in a system with 10 "
                    "partitions Redpanda will attempt to reserve at least 2 GB "
                    "of disk space.",
            .needs_restart = needs_restart::no,
            .example = "4",
            .visibility = visibility::tunable,
          };
      }),
      2,
      {.min = 1})
  , debug_load_slice_warning_depth(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "debug_load_slice_warning_depth",
            .desc = "The recursion depth after which debug logging is enabled "
                    "automatically for the log reader.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      std::nullopt)
  , id_allocator_log_capacity(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "id_allocator_log_capacity",
            .desc
            = "Capacity of the `id_allocator` log in number of batches. After "
              "it reaches `id_allocator_stm`, it truncates the log's prefix.",
            .visibility = visibility::tunable,
          };
      }),
      100)
  , id_allocator_batch_size(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "id_allocator_batch_size",
            .desc = "The ID allocator allocates messages in batches (each "
                    "batch is a one log record) and then serves requests from "
                    "memory without touching the log until the batch is "
                    "exhausted.",
            .visibility = visibility::tunable,
          };
      }),
      1000)
  , enable_sasl(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "enable_sasl",
            .desc = "Enable SASL authentication for Kafka connections. "
                    "Authorization is required to modify this property. See "
                    "also `kafka_enable_authorization`.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      false)
  , sasl_mechanisms(
      *this,
      is_enterprise_sasl_mechanism,
      static_metadata([] {
          return base_property::metadata{
            .name = "sasl_mechanisms",
            .desc = "A list of supported SASL mechanisms, if no override is "
                    "defined in `sasl_mechanisms_overrides` for each Kafka "
                    "listener. Accepted values: `SCRAM`, `GSSAPI`, "
                    "`OAUTHBEARER`, `PLAIN`. Note that in order to enable "
                    "PLAIN, you must also enable SCRAM.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      std::vector<ss::sstring>{ss::sstring{scram}},
      validate_sasl_mechanisms)
  , sasl_mechanisms_overrides(
      *this,
      is_enterprise_sasl_mechanisms_override,
      static_metadata([] {
          return base_property::metadata{
            .name = "sasl_mechanisms_overrides",
            .desc = "A list of overrides for SASL mechanisms, defined by "
                    "listener. SASL mechanisms defined here will replace the "
                    "ones set in `sasl_mechanisms`. The same limitations apply "
                    "as for `sasl_mechanisms`.",
            .needs_restart = needs_restart::no,
            .example
            = "[{'listener':'kafka_listener', 'sasl_mechanisms':['SCRAM']}]",
            .visibility = visibility::user,
          };
      }),
      std::vector<sasl_mechanisms_override>{},
      validate_sasl_mechanisms_overrides)
  , sasl_kerberos_config(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "sasl_kerberos_config",
            .desc
            = "The location of the Kerberos `krb5.conf` file for Redpanda.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      "/etc/krb5.conf")
  , sasl_kerberos_keytab(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "sasl_kerberos_keytab",
            .desc = "The location of the Kerberos keytab file for Redpanda.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      "/var/lib/redpanda/redpanda.keytab")
  , sasl_kerberos_principal(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "sasl_kerberos_principal",
            .desc = "The primary of the Kerberos Service Principal Name (SPN) "
                    "for Redpanda.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      "redpanda",
      &validate_non_empty_string)
  , sasl_kerberos_principal_mapping(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "sasl_kerberos_principal_mapping",
            .desc = "Rules for mapping Kerberos principal names to Redpanda "
                    "user principals.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      {"DEFAULT"},
      security::validate_kerberos_mapping_rules)
  , kafka_sasl_max_reauth_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_sasl_max_reauth_ms",
            .desc
            = "The maximum time between Kafka client reauthentications. If a "
              "client has not reauthenticated a connection within this time "
              "frame, that connection is torn down. If this property is not "
              "set (or set to `null`), session expiry is disabled, and a "
              "connection could live long after the client's credentials are "
              "expired or revoked.",
            .needs_restart = needs_restart::no,
            .example = "1000",
            .visibility = visibility::user,
          };
      }),
      std::nullopt,
      {.min = 1000ms})
  , kafka_enable_authorization(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_enable_authorization",
            .desc
            = "Flag to require authorization for Kafka connections. If `null`, "
              "the property is disabled, and authorization is instead enabled "
              "by enable_sasl. * `null`: Ignored. Authorization is enabled "
              "with `enable_sasl`: `true` * `true`: authorization is required. "
              "* `false`: authorization is disabled.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      std::nullopt)
  , tls_certificate_name_format(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "tls_certificate_name_format",
            .desc
            = "The format of the certificates's distinguished name to use for "
              "mTLS principal mapping. Legacy format would appear as "
              "'C=US,ST=California,L=San Francisco,O=Redpanda,CN=redpanda', "
              "while rfc2253 format would appear as "
              "'CN=redpanda,O=Redpanda,L=San Francisco,ST=California,C=US'.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      tls_name_format::legacy,
      {tls_name_format::legacy, tls_name_format::rfc2253})
  , kafka_mtls_principal_mapping_rules(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_mtls_principal_mapping_rules",
            .desc = "Principal mapping rules for mTLS authentication on the "
                    "Kafka API. If `null`, the property is disabled.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      std::nullopt,
      security::tls::validate_rules)
  , kafka_enable_partition_reassignment(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_enable_partition_reassignment",
            .desc = "Enable the Kafka partition reassignment API.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      true)
  , controller_backend_housekeeping_interval_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "controller_backend_housekeeping_interval_ms",
            .desc = "Interval between iterations of controller backend "
                    "housekeeping loop.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      1s)
  , kafka_request_max_bytes(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_request_max_bytes",
            .desc
            = "Maximum size of a single request processed using the Kafka API.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      100_MiB)
  , kafka_batch_max_bytes(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_batch_max_bytes",
            .desc
            = "Maximum size of a batch processed by the server. If the batch "
              "is compressed, the limit applies to the compressed batch size.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      1_MiB)
  , delete_topic_enable(
      *this,
      false,
      static_metadata([] {
          return base_property::metadata{
            .name = "delete_topic_enable",
            .desc = "Enable or disable topic deletion via the Kafka "
                    "DeleteTopics API. When set to false, all topic deletion "
                    "requests are rejected with error code 73 "
                    "(TOPIC_DELETION_DISABLED). This is a cluster-wide safety "
                    "setting that cannot be overridden by superusers. Topics "
                    "in kafka_nodelete_topics are always protected regardless "
                    "of this setting.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      true)
  , kafka_nodelete_topics(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_nodelete_topics",
            .desc = "A list of topics that are protected from deletion and "
                    "configuration changes by Kafka clients. Set by default to "
                    "a list of Redpanda internal topics.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      {model::kafka_audit_logging_topic(), "__consumer_offsets", "_schemas"},
      &validate_non_empty_string_vec)
  , kafka_noproduce_topics(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_noproduce_topics",
            .desc
            = "A list of topics that are protected from being produced to by "
              "Kafka clients. Set by default to a list of Redpanda internal "
              "topics.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      {},
      &validate_non_empty_string_vec)
  , kafka_topics_max(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_topics_max",
            .desc = "Maximum number of Kafka user topics that can be created. "
                    "If `null`, then no limit is enforced.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      std::nullopt)
  , kafka_max_message_size_upper_limit_bytes(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_max_message_size_upper_limit_bytes",
            .desc = "Maximum allowed value for the `max.message.size` topic "
                    "property. When set to `null`, then no limit is enforced.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      100_MiB,
      {.min = 1})
  , compaction_ctrl_update_interval_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "compaction_ctrl_update_interval_ms",
            .desc = "",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      30s)
  , compaction_ctrl_p_coeff(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "compaction_ctrl_p_coeff",
            .desc = "Proportional coefficient for compaction PID controller. "
                    "This must be negative, because the compaction backlog "
                    "should decrease when the number of compaction shares "
                    "increases.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      -12.5)
  , compaction_ctrl_i_coeff(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "compaction_ctrl_i_coeff",
            .desc = "Integral coefficient for compaction PID controller.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      0.0)
  , compaction_ctrl_d_coeff(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "compaction_ctrl_d_coeff",
            .desc = "Derivative coefficient for compaction PID controller.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      0.2)
  , compaction_ctrl_min_shares(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "compaction_ctrl_min_shares",
            .desc = "Minimum number of I/O and CPU shares that compaction "
                    "process can use.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      10)
  , compaction_ctrl_max_shares(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "compaction_ctrl_max_shares",
            .desc = "Maximum number of I/O and CPU shares that compaction "
                    "process can use.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      1000)
  , compaction_ctrl_backlog_size(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "compaction_ctrl_backlog_size",
            .desc = "Target backlog size for compaction controller. If not set "
                    "the max backlog size is configured to 80% of total disk "
                    "space available.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      std::nullopt)
  , members_backend_retry_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "members_backend_retry_ms",
            .desc = "Time between members backend reconciliation loop retries.",
            .visibility = visibility::tunable,
          };
      }),
      5s)
  , kafka_connections_max(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_connections_max",
            .desc = "Maximum number of Kafka client connections per broker. If "
                    "`null`, the property is disabled.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      std::nullopt)
  , kafka_connections_max_per_ip(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_connections_max_per_ip",
            .desc = "Maximum number of Kafka client connections per IP "
                    "address, per broker. If `null`, the property is disabled.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      std::nullopt)
  , kafka_connections_max_overrides(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_connections_max_overrides",
            .desc
            = "A list of IP addresses for which Kafka client connection limits "
              "are overridden and don't apply. For example, `(['127.0.0.1:90', "
              "'50.20.1.1:40']).`",
            .needs_restart = needs_restart::no,
            .example = R"(['127.0.0.1:90', '50.20.1.1:40'])",
            .visibility = visibility::user,
          };
      }),
      {},
      validate_connection_rate)
  , kafka_rpc_server_tcp_recv_buf(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_rpc_server_tcp_recv_buf",
            .desc = "Size of the Kafka server TCP receive buffer. If `null`, "
                    "the property is disabled.",
            .example = "65536",
          };
      }),
      std::nullopt,
      {.min = 32_KiB, .align = 4_KiB})
  , kafka_rpc_server_tcp_send_buf(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_rpc_server_tcp_send_buf",
            .desc = "Size of the Kafka server TCP transmit buffer. If `null`, "
                    "the property is disabled.",
            .example = "65536",
          };
      }),
      std::nullopt,
      {.min = 32_KiB, .align = 4_KiB})
  , kafka_rpc_server_stream_recv_buf(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_rpc_server_stream_recv_buf",
            .desc = "Maximum size of the user-space receive buffer. If `null`, "
                    "this limit is not applied.",
            .example = "65536",
            .visibility = visibility::tunable,
          };
      }),
      std::nullopt,
      // The minimum is set to match seastar's min_buffer_size (i.e. don't
      // permit setting a max below the min).  The maximum is set to forbid
      // contiguous allocations beyond that size.
      {.min = 512, .max = 512_KiB, .align = 4_KiB})
  , kafka_enable_describe_log_dirs_remote_storage(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_enable_describe_log_dirs_remote_storage",
            .desc = "Whether to include Tiered Storage as a special remote:// "
                    "directory in `DescribeLogDirs Kafka` API requests.",
            .needs_restart = needs_restart::no,
            .example = "false",
            .visibility = visibility::user,
          };
      }),
      true)
  , audit_enabled(
      *this,
      true, /* restricted values */
      static_metadata([] {
          return base_property::metadata{
            .name = "audit_enabled",
            .desc = "Enables or disables audit logging. When you set this to "
                    "true, Redpanda checks for an existing topic named "
                    "`_redpanda.audit_log`. If none is found, Redpanda "
                    "automatically creates one for you.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      false)
  , audit_log_num_partitions(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "audit_log_num_partitions",
            .desc = "Defines the number of partitions used by a newly-created "
                    "audit topic. This configuration applies only to the audit "
                    "log topic and may be different from the cluster or other "
                    "topic configurations. This cannot be altered for existing "
                    "audit log topics.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      12)
  , audit_log_replication_factor(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "audit_log_replication_factor",
            .desc
            = "Defines the replication factor for a newly-created audit log "
              "topic. This configuration applies only to the audit log topic "
              "and may be different from the cluster or other topic "
              "configurations. This cannot be altered for existing audit log "
              "topics. Setting this value is optional. If a value is not "
              "provided, Redpanda will use the value specified for "
              "`internal_topic_replication_factor`.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      std::nullopt)
  , audit_client_max_buffer_size(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "audit_client_max_buffer_size",
            .desc = "Defines the number of bytes allocated by the internal "
                    "audit client for audit messages. When changing this, you "
                    "must disable audit logging and then re-enable it for the "
                    "change to take effect. Consider increasing this if your "
                    "system generates a very large number of audit records in "
                    "a short amount of time.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      16_MiB)
  , audit_queue_drain_interval_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "audit_queue_drain_interval_ms",
            .desc = "Interval, in milliseconds, at which Redpanda flushes the "
                    "queued audit log messages to the audit log topic. Longer "
                    "intervals may help prevent duplicate messages, especially "
                    "in high throughput scenarios, but they also increase the "
                    "risk of data loss during shutdowns where the queue is "
                    "lost.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      500ms)
  , audit_queue_max_buffer_size_per_shard(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "audit_queue_max_buffer_size_per_shard",
            .desc = "Defines the maximum amount of memory in bytes used by the "
                    "audit buffer in each shard. Once this size is reached, "
                    "requests to log additional audit messages will return a "
                    "non-retryable error. Limiting the buffer size per shard "
                    "helps prevent any single shard from consuming excessive "
                    "memory due to audit log messages.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      1_MiB)
  , audit_enabled_event_types(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "audit_enabled_event_types",
            .desc = "List of strings in JSON style identifying the event types "
                    "to include in the audit log. This may include any of the "
                    "following: `management, produce, consume, describe, "
                    "heartbeat, authenticate, schema_registry, admin`.",
            .needs_restart = needs_restart::no,
            .example = R"(["management", "describe"])",
            .visibility = visibility::user,
          };
      }),
      {"management", "authenticate", "admin"},
      validate_audit_event_types)
  , audit_excluded_topics(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "audit_excluded_topics",
            .desc = "List of topics to exclude from auditing.",
            .needs_restart = needs_restart::no,
            .example = R"(["topic1","topic2"])",
            .visibility = visibility::user,
          };
      }),
      {},
      validate_audit_excluded_topics)
  , audit_excluded_principals(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "audit_excluded_principals",
            .desc = "List of user principals to exclude from auditing.",
            .needs_restart = needs_restart::no,
            .example = R"(["User:principal1","User:principal2"])",
            .visibility = visibility::user,
          };
      }),
      {})
  , audit_failure_policy(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "audit_failure_policy",
            .desc = "Defines the policy for rejecting audit log messages when "
                    "the audit log queue is full. If set to 'permit', then new "
                    "audit messages are dropped and the operation is "
                    "permitted. If set to 'reject', then the operation is "
                    "rejected.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      audit_failure_policy::reject,
      {audit_failure_policy::reject, audit_failure_policy::permit})
  , audit_use_rpc(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "audit_use_rpc",
            .desc = "Produce audit log messages using internal Redpanda RPCs. "
                    "When disabled, produce audit log messages using a Kafka "
                    "client instead.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      true)
  , schema_registry_use_rpc(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "schema_registry_use_rpc",
            .desc = "Use internal Redpanda RPCs for schema registry internal "
                    "topic I/O. When disabled, use a Kafka client for schema "
                    "registry internal topic I/O instead.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      true)
  , cloud_storage_enabled(
      *this,
      true,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_enabled",
            .desc = "Enable object storage. Must be set to `true` to use "
                    "Tiered Storage or Remote Read Replicas.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
          };
      }),
      false)
  , cloud_storage_enable_remote_read(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_enable_remote_read",
            .desc = "Default remote read config value for new topics",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , cloud_storage_enable_remote_write(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_enable_remote_write",
            .desc = "Default remote write value for new topics",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , default_redpanda_storage_mode(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "default_redpanda_storage_mode",
            .desc = "Default storage mode for newly-created topics. Determines "
                    "how topic data is stored: `local` for broker-local "
                    "storage only, `tiered` for both local and object storage, "
                    "`cloud` for object-only storage using the Cloud Topics "
                    "architecture, or `unset` to use legacy remote.read/write "
                    "configs for backwards compatibility.",
            .needs_restart = needs_restart::no,
            .example = "tiered",
            .visibility = visibility::user,
          };
      }),
      model::redpanda_storage_mode::unset,
      {model::redpanda_storage_mode::local,
       model::redpanda_storage_mode::tiered,
       model::redpanda_storage_mode::cloud,
       model::redpanda_storage_mode::unset})
  , cloud_storage_disable_archiver_manager(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_disable_archiver_manager",
            .desc = "Use legacy upload mode and do not start archiver_manager.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
          };
      }),
      true)
  , cloud_storage_access_key(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_access_key",
            .desc = "AWS or GCP access key. This access key is part of the "
                    "credentials that Redpanda requires to authenticate with "
                    "object storage services for Tiered Storage. This access "
                    "key is used with the <<cloud_storage_secret_key>> to form "
                    "the complete credentials required for authentication. To "
                    "authenticate using IAM roles, see "
                    "cloud_storage_credentials_source.",
            .visibility = visibility::user,
            .gets_restored = gets_restored::no,
          };
      }),
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_secret_key(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_secret_key",
            .desc = "Cloud provider secret key.",
            .visibility = visibility::user,
            .secret = is_secret::yes,
            .gets_restored = gets_restored::no,
          };
      }),
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_region(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_region",
            .desc = "Cloud provider region that houses the bucket or container "
                    "used for storage.",
            .visibility = visibility::user,
            .gets_restored = gets_restored::no,
          };
      }),
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_bucket(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_bucket",
            .desc = "AWS or GCP bucket or container that should be used to "
                    "store data.",
            .visibility = visibility::user,
            .gets_restored = gets_restored::no,
          };
      }),
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_api_endpoint(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_api_endpoint",
            .desc
            = "Optional API endpoint. - AWS: When blank, this is automatically "
              "generated using <<cloud_storage_region,region>> and "
              "<<cloud_storage_bucket,bucket>>. Otherwise, this uses the value "
              "assigned. - GCP: Uses `storage.googleapis.com`.",
            .visibility = visibility::user,
            .gets_restored = gets_restored::no,
          };
      }),
      std::nullopt,
      &validate_api_endpoint)
  , cloud_storage_url_style(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_url_style",
            .desc
            = "Specifies the addressing style to use for Amazon S3 requests. "
              "This configuration determines how S3 bucket URLs are formatted. "
              "You can choose between: `virtual_host`, (for example, "
              "`<bucket-name>.s3.amazonaws.com`), `path`, (for example, "
              "`s3.amazonaws.com/<bucket-name>`), and `null`. Path style is "
              "supported for backward compatibility with legacy systems. When "
              "this property is not set or is `null`, the client tries to use "
              "`virtual_host` addressing. If the initial request fails, the "
              "client automatically tries the `path` style. If neither "
              "addressing style works, Redpanda terminates the startup, "
              "requiring manual configuration to proceed.",
            .needs_restart = needs_restart::yes,
            .example = "virtual_host",
            .visibility = visibility::user,
          };
      }),
      std::nullopt,
      {s3_url_style::virtual_host, s3_url_style::path, std::nullopt})
  , cloud_storage_credentials_source(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_credentials_source",
            .desc
            = "The source of credentials used to authenticate to object "
              "storage services. Required for cluster provider authentication "
              "with IAM roles. To authenticate using access keys, see "
              "cloud_storage_access_key`. Accepted values: `config_file`, "
              "`aws_instance_metadata`, `sts, gcp_instance_metadata`, "
              "`azure_vm_instance_metadata`, `azure_aks_oidc_federation`",
            .needs_restart = needs_restart::yes,
            .example = "config_file",
            .visibility = visibility::user,
            .gets_restored = gets_restored::no,
          };
      }),
      model::cloud_credentials_source::config_file,
      {
        model::cloud_credentials_source::config_file,
        model::cloud_credentials_source::aws_instance_metadata,
        model::cloud_credentials_source::sts,
        model::cloud_credentials_source::gcp_instance_metadata,
        model::cloud_credentials_source::azure_aks_oidc_federation,
        model::cloud_credentials_source::azure_vm_instance_metadata,
      })
  , cloud_storage_azure_managed_identity_id(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_azure_managed_identity_id",
            .desc = "The managed identity ID to use for access to the Azure "
                    "storage account. To use Azure managed identities, you "
                    "must set `cloud_storage_credentials_source` to "
                    "`azure_vm_instance_metadata`.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_roles_operation_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_roles_operation_timeout_ms",
            .desc = "Timeout for IAM role related operations (ms)",
            .visibility = visibility::tunable,
          };
      }),
      30s)
  , cloud_storage_upload_loop_initial_backoff_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_upload_loop_initial_backoff_ms",
            .desc
            = "Initial backoff interval when there is nothing to upload for a "
              "partition (ms).",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      100ms)
  , cloud_storage_upload_loop_max_backoff_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_upload_loop_max_backoff_ms",
            .desc = "Max backoff interval when there is nothing to upload for "
                    "a partition (ms).",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      10s)
  , cloud_storage_max_connections(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_max_connections",
            .desc = "Maximum simultaneous object storage connections per "
                    "shard, applicable to upload and download activities.",
            .visibility = visibility::user,
          };
      }),
      20)
  , cloud_storage_disable_tls(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_disable_tls",
            .desc = "Disable TLS for all object storage connections.",
            .visibility = visibility::user,
          };
      }),
      false)
  , cloud_storage_api_endpoint_port(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_api_endpoint_port",
            .desc = "TLS port override.",
            .visibility = visibility::user,
          };
      }),
      443)
  , cloud_storage_trust_file(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_trust_file",
            .desc = "Path to certificate that should be used to validate "
                    "server certificate during TLS handshake.",
            .visibility = visibility::user,
            .gets_restored = gets_restored::no,
          };
      }),
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_crl_file(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_crl_file",
            .desc = "Path to certificate revocation list for "
                    "`cloud_storage_trust_file`.",
            .visibility = visibility::user,
          };
      }),
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_initial_backoff_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_initial_backoff_ms",
            .desc
            = "Initial backoff time for exponential backoff algorithm (ms)",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      100ms)
  , cloud_storage_segment_upload_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_segment_upload_timeout_ms",
            .desc = "Log segment upload timeout (ms)",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      90s)
  , cloud_storage_manifest_upload_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_manifest_upload_timeout_ms",
            .desc = "Manifest upload timeout (ms).",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      30s)
  , cloud_storage_garbage_collect_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_garbage_collect_timeout_ms",
            .desc
            = "Timeout for running the cloud storage garbage collection (ms).",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      30s)
  , cloud_storage_max_connection_idle_time_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_max_connection_idle_time_ms",
            .desc = "Max https connection idle time (ms)",
            .visibility = visibility::tunable,
          };
      }),
      5s)
  , cloud_storage_segment_max_upload_interval_sec(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_segment_max_upload_interval_sec",
            .desc = "Time that segment can be kept locally without uploading "
                    "it to the remote storage (sec).",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      1h)
  , cloud_storage_manifest_max_upload_interval_sec(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_manifest_max_upload_interval_sec",
            .desc = "Wait at least this long between partition manifest "
                    "uploads. Actual time between uploads may be greater than "
                    "this interval. If this property is not set, or null, "
                    "metadata will be updated after each segment upload.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      60s)
  , cloud_storage_readreplica_manifest_sync_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_readreplica_manifest_sync_timeout_ms",
            .desc = "Timeout to check if new data is available for partition "
                    "in S3 for read replica.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      30s)
  , cloud_storage_metadata_sync_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_metadata_sync_timeout_ms",
            .desc = "Timeout for SI metadata synchronization.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      10s)
  , cloud_storage_housekeeping_interval_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_housekeeping_interval_ms",
            .desc = "Interval for cloud storage housekeeping tasks.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      5min)
  , cloud_storage_idle_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_idle_timeout_ms",
            .desc = "Timeout used to detect idle state of the cloud storage "
                    "API. If the average cloud storage request rate is below "
                    "this threshold for a configured amount of time the cloud "
                    "storage is considered idle and the housekeeping jobs are "
                    "started.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      10s)
  , cloud_storage_cluster_metadata_upload_interval_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_cluster_metadata_upload_interval_ms",
            .desc = "Time interval to wait between cluster metadata uploads.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      1h)
  , cloud_storage_cluster_metadata_upload_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_cluster_metadata_upload_timeout_ms",
            .desc = "Timeout for cluster metadata uploads.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      60s)
  , cloud_storage_cluster_metadata_num_consumer_groups_per_upload(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name
            = "cloud_storage_cluster_metadata_num_consumer_groups_per_upload",
            .desc = "Number of groups to upload in a single snapshot object "
                    "during consumer offsets upload. Setting a lower value "
                    "will mean a larger number of smaller snapshots are "
                    "uploaded.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      1000)
  , cloud_storage_cluster_metadata_retries(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_cluster_metadata_retries",
            .desc = "Number of attempts metadata operations may be retried.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      5)
  , cloud_storage_attempt_cluster_restore_on_bootstrap(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_attempt_cluster_restore_on_bootstrap",
            .desc = "When set to `true`, Redpanda automatically retrieves "
                    "cluster metadata from a specified object storage bucket "
                    "at the cluster's first startup. This option is ideal for "
                    "orchestrated deployments, such as Kubernetes. Ensure any "
                    "previous cluster linked to the bucket is fully "
                    "decommissioned to prevent conflicts between Tiered "
                    "Storage subsystems.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , cloud_storage_idle_threshold_rps(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_idle_threshold_rps",
            .desc = "The cloud storage request rate threshold for idle state "
                    "detection. If the average request rate for the configured "
                    "period is lower than this threshold the cloud storage is "
                    "considered being idle.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      10.0)
  , cloud_storage_background_jobs_quota(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_background_jobs_quota",
            .desc = "The total number of requests the object storage "
                    "background jobs can make during one background "
                    "housekeeping run. This is a per-shard limit. Adjusting "
                    "this limit can optimize object storage traffic and impact "
                    "shard performance.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      5000)
  , cloud_storage_enable_segment_merging(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_enable_segment_merging",
            .desc = "Enables adjacent segment merging. The segments are "
                    "reuploaded if there is an opportunity for that and if it "
                    "will improve the tiered-storage performance",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      true)
  , cloud_storage_enable_scrubbing(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_enable_scrubbing",
            .desc
            = "Enable scrubbing of cloud storage partitions. The scrubber "
              "validates the integrity of data and metadata uploaded to cloud "
              "storage.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , cloud_storage_partial_scrub_interval_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_partial_scrub_interval_ms",
            .desc
            = "Time interval between two partial scrubs of the same partition.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      1h)
  , cloud_storage_full_scrub_interval_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_full_scrub_interval_ms",
            .desc = "Time interval between a final scrub and the next.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      12h)
  , cloud_storage_scrubbing_interval_jitter_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_scrubbing_interval_jitter_ms",
            .desc = "Jitter applied to the cloud storage scrubbing interval.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      10min)
  , cloud_storage_disable_upload_loop_for_tests(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_disable_upload_loop_for_tests",
            .desc = "Begins the upload loop in tiered-storage-enabled topic "
                    "partitions. The property exists to simplify testing and "
                    "shouldn't be set in production.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , cloud_storage_disable_read_replica_loop_for_tests(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_disable_read_replica_loop_for_tests",
            .desc = "Begins the read replica sync loop in "
                    "tiered-storage-enabled topic partitions. The property "
                    "exists to simplify testing and shouldn't be set in "
                    "production.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , disable_cluster_recovery_loop_for_tests(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "disable_cluster_recovery_loop_for_tests",
            .desc = "Disables the cluster recovery loop. This property is used "
                    "to simplify testing and should not be set in production.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , cloud_topics_disable_metastore_flush_loop_for_tests(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_disable_metastore_flush_loop_for_tests",
            .desc
            = "Disables the metastore flush loop in cloud topics. The property "
              "exists to simplify testing of read replicas and shouldn't be "
              "set in production.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , cloud_topics_disable_level_zero_gc_for_tests(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_disable_level_zero_gc_for_tests",
            .desc
            = "Disables the level-zero garbage collector in cloud topics. This "
              "property exists to simplify testing and shouldn't be set in "
              "production.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , enable_cluster_metadata_upload_loop(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "enable_cluster_metadata_upload_loop",
            .desc = "Enables cluster metadata uploads. Required for whole "
                    "cluster restore.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      // TODO: make this runtime configurable.
      true)
  , cloud_storage_cluster_name(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_cluster_name",
            .desc = "Optional unique name to disambiguate this cluster's "
                    "metadata in object storage (e.g. for Whole Cluster "
                    "Restore) when multiple clusters share a bucket. Must be "
                    "unique within the bucket, 1-64 chars, [A-Za-z0-9_-]. Do "
                    "not change once set.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
            // Do not restore this value from the existing cluster metadata. It
            // may be empty if the metadata is slightly stale. It may be
            // different if we are trying to restore from a cluster with a
            // different name.
            .gets_restored = gets_restored::no,
          };
      }),
      std::nullopt,
      &validate_cloud_storage_cluster_name)
  , cloud_storage_max_segments_pending_deletion_per_partition(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_max_segments_pending_deletion_per_partition",
            .desc = "The per-partition limit for the number of segments "
                    "pending deletion from the cloud. Segments can be deleted "
                    "due to retention or compaction. If this limit is breached "
                    "and deletion fails, then segments will be orphaned in the "
                    "cloud and will have to be removed manually. Applies only "
                    "the the in-memory manifest. Spillover manifests are not "
                    "affected by this limit.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      5000)
  , cloud_storage_gc_max_segments_per_run(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_gc_max_segments_per_run",
            .desc = "Maximum number of segments to delete per housekeeping "
                    "run. Each segment maps to up to three object keys (data, "
                    "index, tx manifest), so a value of 300 produces 600 to "
                    "900 deletes plus one per spillover manifest.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      300,
      {.min = 1})
  , cloud_storage_enable_compacted_topic_reupload(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_enable_compacted_topic_reupload",
            .desc = "Enable re-uploading data for compacted topics",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      true)
  , cloud_storage_recovery_temporary_retention_bytes_default(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_recovery_temporary_retention_bytes_default",
            .desc
            = "Retention in bytes for topics created during automated recovery",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      1_GiB)
  , cloud_storage_recovery_topic_validation_mode(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_recovery_topic_validation_mode",
            .desc
            = "Validation performed before recovering a topic from object "
              "storage. In case of failure, the reason for the failure appears "
              "as `ERROR` lines in the Redpanda application log. For each "
              "topic, this reports errors for all partitions, but for each "
              "partition, only the first error is reported. This property "
              "accepts the following parameters: `no_check`: Skips the checks "
              "for topic recovery. `check_manifest_existence`: Runs an "
              "existence check on each `partition_manifest`. Fails if there "
              "are connection issues to the object storage. "
              "`check_manifest_and_segment_metadata`: Downloads the manifest "
              "and runs a consistency check, comparing the metadata with the "
              "cloud storage objects. The process fails if metadata references "
              "any missing cloud storage objects.",
            .needs_restart = needs_restart::no,
            .example = "check_manifest_existence",
            .visibility = visibility::tunable,
          };
      }),
      model::recovery_validation_mode::check_manifest_existence,
      {model::recovery_validation_mode::check_manifest_existence,
       model::recovery_validation_mode::check_manifest_and_segment_metadata,
       model::recovery_validation_mode::no_check})
  , cloud_storage_recovery_topic_validation_depth(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_recovery_topic_validation_depth",
            .desc = "Number of metadata segments to validate, from newest to "
                    "oldest, when "
                    "`cloud_storage_recovery_topic_validation_mode` is set to "
                    "`check_manifest_and_segment_metadata`.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      10)
  , cloud_storage_client_lease_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_client_lease_timeout_ms",
            .desc = "Maximum time to hold a cloud storage client lease (ms), "
                    "after which any outstanding connection is immediately "
                    "closed.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      900s)
  , cloud_storage_segment_size_target(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_segment_size_target",
            .desc = "Desired segment size in the cloud storage. Default: "
                    "segment.bytes",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      std::nullopt)
  , cloud_storage_segment_size_min(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_segment_size_min",
            .desc
            = "Smallest acceptable segment size in the cloud storage. Default: "
              "cloud_storage_segment_size_target/2",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      std::nullopt)
  , cloud_storage_max_throughput_per_shard(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_max_throughput_per_shard",
            .desc
            = "Max throughput used by tiered-storage per shard in bytes per "
              "second. This value is an upper bound of the throughput "
              "available to the tiered-storage subsystem. This parameter is "
              "intended to be used as a safeguard and in tests when we need to "
              "set precise throughput value independent of actual storage "
              "media. Please use 'cloud_storage_throughput_limit_percent' "
              "instead of this parameter in the production environment.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      1_GiB)
  , cloud_storage_throughput_limit_percent(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_throughput_limit_percent",
            .desc
            = "Max throughput used by tiered-storage per node expressed as a "
              "percentage of the disk bandwidth. If the server has several "
              "disks Redpanda will take into account only the one which is "
              "used to store tiered-storage cache. Note that even if the "
              "tiered-storage is allowed to use full bandwidth of the disk "
              "(100%) it won't necessary use it in full. The actual usage "
              "depend on your workload and the state of the tiered-storage "
              "cache. This parameter is a safeguard that prevents "
              "tiered-storage from using too many system resources and not a "
              "performance tuning knob.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      50,
      {.min = 0, .max = 100})
  , cloud_storage_graceful_transfer_timeout_ms(
      *this,
      static_metadata([] {
          static constexpr std::string_view _aliases[] = {
            "cloud_storage_graceful_transfer_timeout"};
          return base_property::metadata{
            .name = "cloud_storage_graceful_transfer_timeout_ms",
            .desc = "Time limit on waiting for uploads to complete before a "
                    "leadership transfer. If this is null, leadership "
                    "transfers will proceed without waiting.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
            .aliases = _aliases,
          };
      }),
      5s)
  , cloud_storage_backend(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_backend",
            .desc
            = "Optional object storage backend variant used to select API "
              "capabilities. If not supplied, this will be inferred from other "
              "configuration properties.",
            .needs_restart = needs_restart::yes,
            .example = "aws",
            .visibility = visibility::user,
            .gets_restored = gets_restored::no,
          };
      }),
      model::cloud_storage_backend::unknown,
      {model::cloud_storage_backend::aws,
       model::cloud_storage_backend::google_s3_compat,
       model::cloud_storage_backend::azure,
       model::cloud_storage_backend::minio,
       model::cloud_storage_backend::oracle_s3_compat,
       model::cloud_storage_backend::linode_s3_compat,
       model::cloud_storage_backend::unknown})
  , cloud_storage_credentials_host(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_credentials_host",
            .desc = "The hostname to connect to for retrieving role based "
                    "credentials. Derived from "
                    "cloud_storage_credentials_source if not set. Only "
                    "required when using IAM role based access. To "
                    "authenticate using access keys, see "
                    "`cloud_storage_access_key`.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
            .gets_restored = gets_restored::no,
          };
      }),
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_spillover_manifest_size(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_spillover_manifest_size",
            .desc = "The size of the manifest which can be offloaded to the "
                    "cloud. If the size of the local manifest stored in "
                    "redpanda exceeds cloud_storage_spillover_manifest_size x2 "
                    "the spillover mechanism will split the manifest into two "
                    "parts and one of them will be uploaded to S3.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      64_KiB,
      {.min = 4_KiB, .max = 4_MiB})
  , cloud_storage_spillover_manifest_max_segments(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_spillover_manifest_max_segments",
            .desc
            = "Maximum number of elements in the spillover manifest that can "
              "be offloaded to the cloud storage. This property is similar to "
              "'cloud_storage_spillover_manifest_size' but it triggers "
              "spillover based on number of segments instead of the size of "
              "the manifest in bytes. The property exists to simplify testing "
              "and shouldn't be set in the production environment",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      std::nullopt)
  , cloud_storage_manifest_cache_size(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_manifest_cache_size",
            .desc = "Amount of memory that can be used to handle "
                    "tiered-storage metadata",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      1_MiB,
      {.min = 64_KiB, .max = 64_MiB})
  , cloud_storage_manifest_cache_ttl_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_materialized_manifest_ttl_ms",
            .desc = "The time interval that determines how long the "
                    "materialized manifest can stay in cache under contention. "
                    "This parameter is used for performance tuning. When the "
                    "spillover manifest is materialized and stored in cache "
                    "and the cache needs to evict it it will use "
                    "'cloud_storage_materialized_manifest_ttl_ms' value as a "
                    "timeout. The cursor that uses the spillover manifest uses "
                    "this value as a TTL interval after which it stops "
                    "referencing the manifest making it available for "
                    "eviction. This only affects spillover manifests under "
                    "contention.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      10s)
  , cloud_storage_topic_purge_grace_period_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_topic_purge_grace_period_ms",
            .desc = "Grace period during which the purger will refuse to purge "
                    "the topic.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      30s)
  , cloud_storage_disable_upload_consistency_checks(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_disable_upload_consistency_checks",
            .desc
            = "Disable all upload consistency checks. This will allow redpanda "
              "to upload logs with gaps and replicate metadata with "
              "consistency violations. Normally, this options should be "
              "disabled.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , cloud_storage_disable_archival_stm_rw_fence(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_disable_archival_stm_rw_fence",
            .desc = "Disable concurrency control mechanism in the "
                    "Tiered-Storage. This could potentially break the metadata "
                    "consistency and shouldn't be used in production systems. "
                    "It should only be used for testing.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , cloud_storage_hydration_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_hydration_timeout_ms",
            .desc = "Duration to wait for a hydration request to be fulfilled, "
                    "if hydration is not completed within this time, the "
                    "consumer will be notified with a timeout error.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      600s)
  , cloud_storage_disable_remote_labels_for_tests(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_disable_remote_labels_for_tests",
            .desc = "If 'true', Redpanda disables remote labels and falls back "
                    "on the hash-based object naming scheme for new topics. "
                    "This property exists to simplify testing and shouldn't be "
                    "set in production.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , cloud_storage_enable_segment_uploads(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_enable_segment_uploads",
            .desc = "Controls the upload of log segments to Tiered Storage. If "
                    "set to false, this property temporarily pauses all log "
                    "segment uploads from the Redpanda cluster. When the "
                    "uploads are paused, the "
                    "'cloud_storage_enable_remote_allow_gaps' cluster "
                    "configuration and 'redpanda.remote.allowgaps' topic "
                    "properties control local retention behavior.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      true)
  , cloud_storage_enable_remote_allow_gaps(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_enable_remote_allow_gaps",
            .desc = "Controls the eviction of locally-stored log segments when "
                    "Tiered Storage uploads are paused. Set to `false` "
                    "(default) to only evict data that has already been "
                    "uploaded to cloud storage. If the retained data fills the "
                    "local volume, Redpanda will throttle producers. Set to "
                    "`true` to allow the eviction of locally-stored log "
                    "segments, which may create gaps in offsets.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , cloud_storage_azure_storage_account(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_azure_storage_account",
            .desc = "The name of the Azure storage account to use with Tiered "
                    "Storage. If `null`, the property is disabled.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
            .gets_restored = gets_restored::no,
          };
      }),
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_azure_container(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_azure_container",
            .desc = "The name of the Azure container to use with Tiered "
                    "Storage. If `null`, the property is disabled. The "
                    "container must belong to "
                    "cloud_storage_azure_storage_account.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
            .gets_restored = gets_restored::no,
          };
      }),
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_azure_shared_key(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_azure_shared_key",
            .desc = "The shared key to be used for Azure Shared Key "
                    "authentication with the Azure storage account configured "
                    "by `cloud_storage_azure_storage_account`. If `null`, the "
                    "property is disabled. Redpanda expects this key string to "
                    "be Base64 encoded.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
            .secret = is_secret::yes,
            .gets_restored = gets_restored::no,
          };
      }),
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_azure_adls_endpoint(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_azure_adls_endpoint",
            .desc = "Azure Data Lake Storage v2 endpoint override. Use when "
                    "hierarchical namespaces are enabled on your storage "
                    "account and you have set up a custom endpoint.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
            .gets_restored = gets_restored::no,
          };
      }),
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_azure_adls_port(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_azure_adls_port",
            .desc = "Azure Data Lake Storage v2 port override. See also "
                    "`cloud_storage_azure_adls_endpoint`. Use when "
                    "Hierarchical Namespaces are enabled on your storage "
                    "account and you have set up a custom endpoint.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
            .gets_restored = gets_restored::no,
          };
      }),
      std::nullopt)
  , cloud_storage_azure_hierarchical_namespace_enabled(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_azure_hierarchical_namespace_enabled",
            .desc = "Whether or not an Azure hierarchical namespace is enabled "
                    "on the `cloud_storage_azure_storage_account`. If this "
                    "property is not set, ´cloud_storage_azure_shared_key` "
                    "must be set, and each node tries to determine at startup "
                    "if a hierarchical namespace is enabled. Setting this "
                    "property to `true` disables the check and treats a "
                    "hierarchical namespace as active. Setting to `false` "
                    "disables the check and treats a hierarchical namespace as "
                    "not active.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      std::nullopt)
  , cloud_storage_upload_ctrl_update_interval_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_upload_ctrl_update_interval_ms",
            .desc = "",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      60s)
  , cloud_storage_upload_ctrl_p_coeff(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_upload_ctrl_p_coeff",
            .desc = "proportional coefficient for upload PID controller",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      -2.0)
  , cloud_storage_upload_ctrl_d_coeff(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_upload_ctrl_d_coeff",
            .desc = "derivative coefficient for upload PID controller.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      0.0)
  , cloud_storage_upload_ctrl_min_shares(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_upload_ctrl_min_shares",
            .desc = "minimum number of IO and CPU shares that archival upload "
                    "can use",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      100)
  , cloud_storage_upload_ctrl_max_shares(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_upload_ctrl_max_shares",
            .desc = "maximum number of IO and CPU shares that archival upload "
                    "can use",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      1000)
  , retention_local_target_bytes_default(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "retention_local_target_bytes_default",
            .desc
            = "Local retention size target for partitions of topics with "
              "object storage write enabled. If `null`, the property is "
              "disabled. This property can be overridden on a per-topic basis "
              "by setting `retention.local.target.bytes` in each topic enabled "
              "for Tiered Storage. Both `retention_local_target_bytes_default` "
              "and `retention_local_target_ms_default` can be set. The limit "
              "that is reached earlier is applied.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      std::nullopt)
  , retention_local_target_ms_default(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "retention_local_target_ms_default",
            .desc
            = "Local retention time target for partitions of topics with "
              "object storage write enabled. This property can be overridden "
              "on a per-topic basis by setting `retention.local.target.ms` in "
              "each topic enabled for Tiered Storage. Both "
              "`retention_local_target_bytes_default` and "
              "`retention_local_target_ms_default` can be set. The limit that "
              "is reached first is applied.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      24h)
  , retention_local_strict(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "retention_local_strict",
            .desc = "Flag to allow Tiered Storage topics to expand to "
                    "consumable retention policy limits. When this flag is "
                    "enabled, non-local retention settings are used, and local "
                    "retention settings are used to inform data removal "
                    "policies in low-disk space scenarios.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      false,
      property<bool>::noop_validator,
      legacy_default<bool>(true, legacy_version{9}))
  , retention_local_strict_override(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "retention_local_strict_override",
            .desc = "Trim log data when a cloud topic reaches its local "
                    "retention limit. When this option is disabled Redpanda "
                    "will allow partitions to grow past the local retention "
                    "limit, and will be trimmed automatically as storage "
                    "reaches the configured target size.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      true)
  , retention_local_target_capacity_bytes(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "retention_local_target_capacity_bytes",
            .desc
            = "The target capacity (in bytes) that log storage will try to use "
              "before additional retention rules take over to trim data to "
              "meet the target. When no target is specified, storage usage is "
              "unbounded. Redpanda Data recommends setting only one of "
              "`retention_local_target_capacity_bytes` or "
              "`retention_local_target_capacity_percent`. If both are set, the "
              "minimum of the two is used as the effective target capacity.",
            .needs_restart = needs_restart::no,
            .example = "2147483648000",
            .visibility = visibility::user,
          };
      }),
      std::nullopt,
      property<std::optional<size_t>>::noop_validator,
      legacy_default<std::optional<size_t>>(std::nullopt, legacy_version{9}))
  , retention_local_target_capacity_percent(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "retention_local_target_capacity_percent",
            .desc = "The target capacity in percent of unreserved space "
                    "(`disk_reservation_percent`) that log storage will try to "
                    "use before additional retention rules will take over to "
                    "trim data in order to meet the target. When no target is "
                    "specified storage usage is unbounded. Redpanda Data "
                    "recommends setting only one of "
                    "`retention_local_target_capacity_bytes` or "
                    "`retention_local_target_capacity_percent`. If both are "
                    "set, the minimum of the two is used as the effective "
                    "target capacity.",
            .needs_restart = needs_restart::no,
            .example = "80.0",
            .visibility = visibility::user,
          };
      }),
      80.0,
      {.min = 0.0, .max = 100.0},
      legacy_default<std::optional<double>>(std::nullopt, legacy_version{9}))
  , retention_local_trim_interval(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "retention_local_trim_interval",
            .desc = "The period during which disk usage is checked for disk "
                    "pressure, and data is optionally trimmed to meet the "
                    "target.",
            .needs_restart = needs_restart::no,
            .example = "31536000000",
            .visibility = visibility::tunable,
          };
      }),
      30s)
  , retention_local_trim_overage_coeff(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "retention_local_trim_overage_coeff",
            .desc = "The space management control loop reclaims the overage "
                    "multiplied by this this coefficient to compensate for "
                    "data that is written during the idle period between "
                    "control loop invocations.",
            .needs_restart = needs_restart::no,
            .example = "1.8",
            .visibility = visibility::tunable,
          };
      }),
      2.0)
  , space_management_enable(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "space_management_enable",
            .desc = "Option to explicitly disable automatic disk space "
                    "management. If this property was explicitly disabled "
                    "while using v23.2, it will remain disabled following an "
                    "upgrade.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      true)
  , space_management_enable_override(*this, static_metadata([] {
      return base_property::metadata{
        .name = "space_management_enable_override",
        .visibility = visibility::deprecated,
      };
  }))
  , disk_reservation_percent(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "disk_reservation_percent",
            .desc
            = "The percentage of total disk capacity that Redpanda will avoid "
              "using. This applies both when cloud cache and log data share a "
              "disk, as well as when cloud cache uses a dedicated disk. It is "
              "recommended to not run disks near capacity to avoid blocking "
              "I/O due to low disk space, as well as avoiding performance "
              "issues associated with SSD garbage collection.",
            .needs_restart = needs_restart::no,
            .example = "25.0",
            .visibility = visibility::tunable,
          };
      }),
      25.0,
      {.min = 0.0, .max = 100.0},
      legacy_default<double>(0.0, legacy_version{9}))
  , space_management_max_log_concurrency(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "space_management_max_log_concurrency",
            .desc = "Maximum parallel logs inspected during space management "
                    "process.",
            .needs_restart = needs_restart::no,
            .example = "20",
            .visibility = visibility::tunable,
          };
      }),
      20,
      {.min = 1})
  , space_management_max_segment_concurrency(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "space_management_max_segment_concurrency",
            .desc = "Maximum parallel segments inspected during space "
                    "management process.",
            .needs_restart = needs_restart::no,
            .example = "10",
            .visibility = visibility::tunable,
          };
      }),
      10,
      {.min = 1})
  , initial_retention_local_target_bytes_default(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "initial_retention_local_target_bytes_default",
            .desc
            = "Initial local retention size target for partitions of topics "
              "with Tiered Storage enabled. If no initial local target "
              "retention is configured all locally retained data will be "
              "delivered to learner when joining partition replica set.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      std::nullopt)
  , initial_retention_local_target_ms_default(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "initial_retention_local_target_ms_default",
            .desc
            = "Initial local retention time target for partitions of topics "
              "with Tiered Storage enabled. If no initial local target "
              "retention is configured all locally retained data will be "
              "delivered to learner when joining partition replica set.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      std::nullopt)
  , cloud_storage_cache_size(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_cache_size",
            .desc
            = "Maximum size of object storage cache. If both this property and "
              "cloud_storage_cache_size_percent are set, Redpanda uses the "
              "minimum of the two.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
            .gets_restored = gets_restored::no,
          };
      }),
      0,
      property<uint64_t>::noop_validator,
      legacy_default<uint64_t>(20_GiB, legacy_version{9}))
  , cloud_storage_cache_size_percent(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_cache_size_percent",
            .desc = "Maximum size of the cloud cache as a percentage of "
                    "unreserved disk space disk_reservation_percent. The "
                    "default value for this option is tuned for a shared disk "
                    "configuration. Consider increasing the value if using a "
                    "dedicated cache disk. The property "
                    "<<cloud_storage_cache_size,`cloud_storage_cache_size`>> "
                    "controls the same limit expressed as a fixed number of "
                    "bytes. If both `cloud_storage_cache_size` and "
                    "`cloud_storage_cache_size_percent` are set, Redpanda uses "
                    "the minimum of the two.",
            .needs_restart = needs_restart::no,
            .example = "20.0",
            .visibility = visibility::user,
          };
      }),
      20.0,
      {.min = 0.0, .max = 100.0},
      legacy_default<std::optional<double>>(std::nullopt, legacy_version{9}))
  , cloud_storage_cache_max_objects(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_cache_max_objects",
            .desc = "Maximum number of objects that may be held in the Tiered "
                    "Storage cache. This applies simultaneously with "
                    "`cloud_storage_cache_size`, and whichever limit is hit "
                    "first will trigger trimming of the cache.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      // Enough for a >1TiB cache of 16MiB objects.  Decrease this in case
      // of issues with trim performance.
      100000)
  , cloud_storage_cache_trim_carryover_bytes(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_cache_trim_carryover_bytes",
            .desc = "The cache performs a recursive directory inspection "
                    "during the cache trim. The information obtained during "
                    "the inspection can be carried over to the next trim "
                    "operation. This parameter sets a limit on the memory "
                    "occupied by objects that can be carried over from one "
                    "trim to next, and allows cache to quickly unblock readers "
                    "before starting the directory inspection (deprecated)",
            .needs_restart = needs_restart::no,
            .visibility = visibility::deprecated,
          };
      }),
      // This roughly translates to around 1000 carryover file names
      0_KiB)
  , cloud_storage_cache_check_interval_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_cache_check_interval",
            .desc
            = "Minimum interval between Tiered Storage cache trims, measured "
              "in milliseconds. This setting dictates the cooldown period "
              "after a cache trim operation before another trim can occur. If "
              "a cache fetch operation requests a trim but the interval since "
              "the last trim has not yet passed, the trim will be postponed "
              "until this cooldown expires. Adjusting this interval helps "
              "manage the balance between cache size and retrieval "
              "performance.",
            .visibility = visibility::tunable,
          };
      }),
      5s)
  , cloud_storage_cache_trim_walk_concurrency(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_cache_trim_walk_concurrency",
            .desc = "The maximum number of concurrent tasks launched for "
                    "directory walk during cache trimming. A higher number "
                    "allows cache trimming to run faster but can cause latency "
                    "spikes due to increased pressure on I/O subsystem and "
                    "syscall threads.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      1,
      {.min = 1, .max = 1000})
  , cloud_storage_max_segment_readers_per_shard(
      *this,
      static_metadata([] {
          static constexpr std::string_view _aliases[] = {
            "cloud_storage_max_readers_per_shard"};
          return base_property::metadata{
            .name = "cloud_storage_max_segment_readers_per_shard",
            .desc
            = "Maximum concurrent I/O cursors of materialized remote segments "
              "per CPU core. If unset, value of `topic_partitions_per_shard` "
              "is used, i.e. one segment reader per partition if the shard is "
              "at its maximum partition capacity. These readers are "
              "cachedacross Kafka consume requests and store a readahead "
              "buffer.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
            .aliases = _aliases,
          };
      }),
      std::nullopt)
  , cloud_storage_max_partition_readers_per_shard(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_max_partition_readers_per_shard",
            .desc = "Maximum partition readers per shard (deprecated)",
            .needs_restart = needs_restart::no,
            .visibility = visibility::deprecated,
          };
      }),
      std::nullopt)
  , cloud_storage_max_concurrent_hydrations_per_shard(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_max_concurrent_hydrations_per_shard",
            .desc = "Maximum concurrent segment hydrations of remote data per "
                    "CPU core. If unset, value of "
                    "`cloud_storage_max_connections / 2` is used, which means "
                    "that half of available S3 bandwidth could be used to "
                    "download data from S3. If the cloud storage cache is "
                    "empty every new segment reader will require a download. "
                    "This will lead to 1:1 mapping between number of "
                    "partitions scanned by the fetch request and number of "
                    "parallel downloads. If this value is too large the "
                    "downloads can affect other workloads. In case of any "
                    "problem caused by the tiered-storage reads this value can "
                    "be lowered. This will only affect segment hydrations "
                    "(downloads) but won't affect cached segments. If fetch "
                    "request is reading from the tiered-storage cache its "
                    "concurrency will only be limited by available memory.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      std::nullopt)
  , cloud_storage_max_materialized_segments_per_shard(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_max_materialized_segments_per_shard",
            .desc
            = "Maximum concurrent readers of remote data per CPU core. If "
              "unset, value of `topic_partitions_per_shard` multiplied by 2 is "
              "used.",
            .visibility = visibility::deprecated,
          };
      }),
      std::nullopt)
  , cloud_storage_cache_chunk_size(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_cache_chunk_size",
            .desc = "Size of chunks of segments downloaded into object storage "
                    "cache. Reduces space usage by only downloading the "
                    "necessary chunk from a segment.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      16_MiB)
  , cloud_storage_hydrated_chunks_per_segment_ratio(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_hydrated_chunks_per_segment_ratio",
            .desc = "The maximum number of chunks per segment that can be "
                    "hydrated at a time. Above this number, unused chunks will "
                    "be trimmed.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      0.7)
  , cloud_storage_min_chunks_per_segment_threshold(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_min_chunks_per_segment_threshold",
            .desc = "The minimum number of chunks per segment for trimming to "
                    "be enabled. If the number of chunks in a segment is below "
                    "this threshold, the segment is small enough that all "
                    "chunks in it can be hydrated at any given time",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      5)
  , cloud_storage_disable_chunk_reads(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_disable_chunk_reads",
            .desc = "Disable chunk reads and switch back to legacy mode where "
                    "full segments are downloaded.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , cloud_storage_chunk_eviction_strategy(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_chunk_eviction_strategy",
            .desc = "Selects a strategy for evicting unused cache chunks.",
            .needs_restart = needs_restart::no,
            .example = "eager",
            .visibility = visibility::tunable,
          };
      }),
      model::cloud_storage_chunk_eviction_strategy::eager,
      {model::cloud_storage_chunk_eviction_strategy::eager,
       model::cloud_storage_chunk_eviction_strategy::capped,
       model::cloud_storage_chunk_eviction_strategy::predictive})
  , cloud_storage_chunk_prefetch(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_chunk_prefetch",
            .desc
            = "Number of chunks to prefetch ahead of every downloaded chunk",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      0)
  , cloud_storage_prefetch_segments_max(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_prefetch_segments_max",
            .desc = "Maximum number of small segments (size <= chunk size) to "
                    "prefetch ahead during sequential reads. Set to 0 to "
                    "disable cross-segment prefetch.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      3)
  , cloud_storage_cache_num_buckets(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_cache_num_buckets",
            .desc = "Divide the object storage cache across the specified "
                    "number of buckets. This only works for objects with "
                    "randomized prefixes. The names are not changed when the "
                    "value is set to zero.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      0,
      {.min = 0, .max = 1024})
  , cloud_storage_cache_trim_threshold_percent_size(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_cache_trim_threshold_percent_size",
            .desc = "Trim is triggered when the cache reaches this percent of "
                    "the maximum cache size. If this is unset, the default "
                    "behavioris to start trim when the cache is about 100% "
                    "full.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      std::nullopt,
      {.min = 1.0, .max = 100.0})
  , cloud_storage_cache_trim_threshold_percent_objects(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_cache_trim_threshold_percent_objects",
            .desc = "Trim is triggered when the cache reaches this percent of "
                    "the maximum object count. If this is unset, the default "
                    "behavioris to start trim when the cache is about 100% "
                    "full.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      std::nullopt,
      {.min = 1.0, .max = 100.0})
  , cloud_storage_inventory_based_scrub_enabled(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_inventory_based_scrub_enabled",
            .desc = "Scrubber uses the latest cloud storage inventory report, "
                    "if available, to check if the required objects exist in "
                    "the bucket or container.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , cloud_storage_inventory_id(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_inventory_id",
            .desc = "The name of the scheduled inventory job created by "
                    "Redpanda to generate bucket or container inventory "
                    "reports.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      "redpanda_scrubber_inventory")
  , cloud_storage_inventory_reports_prefix(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_inventory_reports_prefix",
            .desc = "The prefix to the path in the cloud storage bucket or "
                    "container where inventory reports will be placed.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      "redpanda_scrubber_inventory")
  , cloud_storage_inventory_self_managed_report_config(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_inventory_self_managed_report_config",
            .desc = "If enabled, Redpanda will not attempt to create the "
                    "scheduled report configuration using cloud storage APIs. "
                    "The scrubbing process will look for reports in the "
                    "expected paths in the bucket or container, and use the "
                    "latest report found. Primarily intended for use in "
                    "testing and on backends where scheduled inventory reports "
                    "are not supported.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , cloud_storage_inventory_report_check_interval_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_inventory_report_check_interval_ms",
            .desc = "Time interval between checks for a new inventory report "
                    "in the cloud storage bucket or container.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      6h)
  , cloud_storage_inventory_max_hash_size_during_parse(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_storage_inventory_max_hash_size_during_parse",
            .desc = "Maximum bytes of hashes which will be held in memory "
                    "before writing data to disk during inventory report "
                    "parsing. Affects the number of files written by inventory "
                    "service to disk during report parsing, as when this limit "
                    "is reached new files are written to disk.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      64_MiB)
  , superusers(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "superusers",
            .desc = "List of superuser usernames.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      {})
  , kafka_qdc_latency_alpha(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_qdc_latency_alpha",
            .desc = "Smoothing parameter for Kafka queue depth control latency "
                    "tracking.",
            .visibility = visibility::tunable,
          };
      }),
      0.002)
  , kafka_qdc_window_size_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_qdc_window_size_ms",
            .desc
            = "Window size for Kafka queue depth control latency tracking.",
            .visibility = visibility::tunable,
          };
      }),
      1500ms)
  , kafka_qdc_window_count(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_qdc_window_count",
            .desc = "Number of windows used in kafka queue depth control "
                    "latency tracking.",
            .visibility = visibility::tunable,
          };
      }),
      12)
  , kafka_qdc_enable(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_qdc_enable",
            .desc = "Enable kafka queue depth control.",
            .visibility = visibility::user,
          };
      }),
      false)
  , kafka_qdc_depth_alpha(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_qdc_depth_alpha",
            .desc
            = "Smoothing factor for Kafka queue depth control depth tracking.",
            .visibility = visibility::tunable,
          };
      }),
      0.8)
  , kafka_qdc_max_latency_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_qdc_max_latency_ms",
            .desc = "Maximum latency threshold for Kafka queue depth control "
                    "depth tracking.",
            .visibility = visibility::user,
          };
      }),
      80ms)
  , kafka_qdc_idle_depth(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_qdc_idle_depth",
            .desc = "Queue depth when idleness is detected in Kafka queue "
                    "depth control.",
            .visibility = visibility::tunable,
          };
      }),
      10)
  , kafka_qdc_min_depth(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_qdc_min_depth",
            .desc = "Minimum queue depth used in Kafka queue depth control.",
            .visibility = visibility::tunable,
          };
      }),
      1)
  , kafka_qdc_max_depth(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_qdc_max_depth",
            .desc = "Maximum queue depth used in kafka queue depth control.",
            .visibility = visibility::tunable,
          };
      }),
      100)
  , kafka_qdc_depth_update_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_qdc_depth_update_ms",
            .desc = "Update frequency for Kafka queue depth control.",
            .visibility = visibility::tunable,
          };
      }),
      7s)
  , zstd_decompress_workspace_bytes(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "zstd_decompress_workspace_bytes",
            .desc = "Size of the zstd decompression workspace.",
            .visibility = visibility::tunable,
          };
      }),
      8_MiB)
  , lz4_decompress_reusable_buffers_disabled(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "lz4_decompress_reusable_buffers_disabled",
            .desc
            = "Disable reusable preallocated buffers for LZ4 decompression.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , enable_auto_rebalance_on_node_add(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "enable_auto_rebalance_on_node_add",
            .desc
            = "Enable automatic partition rebalancing when new nodes are added",
            .needs_restart = needs_restart::no,
            .visibility = visibility::deprecated,
          };
      }),
      false)
  , partition_autobalancing_mode(
      *this,
      model::partition_autobalancing_mode::continuous,
      model::partition_autobalancing_mode::node_add,
      static_metadata([] {
          return base_property::metadata{
            .name = "partition_autobalancing_mode",
            .desc
            = "Mode of partition balancing for a cluster. * `node_add`: "
              "partition balancing happens when a node is added. * "
              "`continuous`: partition balancing happens automatically to "
              "maintain optimal performance and availability, based on "
              "continuous monitoring for node changes (same as `node_add`) and "
              "also high disk usage. This option requires an Enterprise "
              "license, and it is customized by "
              "`partition_autobalancing_node_availability_timeout_sec` and "
              "`partition_autobalancing_max_disk_usage_percent` properties. * "
              "`off`: partition balancing is disabled. This option is not "
              "recommended for production clusters.",
            .needs_restart = needs_restart::no,
            .example = "node_add",
            .visibility = visibility::user,
          };
      }),
      model::partition_autobalancing_mode::continuous,
      std::vector<model::partition_autobalancing_mode>{
        model::partition_autobalancing_mode::off,
        model::partition_autobalancing_mode::node_add,
        model::partition_autobalancing_mode::continuous,
      },
      legacy_default<model::partition_autobalancing_mode>{
        model::partition_autobalancing_mode::node_add, legacy_version{16}})
  , partition_autobalancing_node_availability_timeout_sec(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "partition_autobalancing_node_availability_timeout_sec",
            .desc
            = "When a node is unavailable for at least this timeout duration, "
              "it triggers Redpanda to move partitions off of the node. This "
              "property applies only when `partition_autobalancing_mode` is "
              "set to `continuous`.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      15min)
  , partition_autobalancing_node_autodecommission_timeout_sec(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "partition_autobalancing_node_autodecommission_timeout_sec",
            .desc
            = "When a node is unavailable for at least this timeout duration, "
              "it triggers Redpanda to decommission the node. This property "
              "applies only when `partition_autobalancing_mode` is set to "
              "`continuous`.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      std::nullopt)
  , partition_autobalancing_max_disk_usage_percent(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "partition_autobalancing_max_disk_usage_percent",
            .desc = "When the disk usage of a node exceeds this threshold, it "
                    "triggers Redpanda to move partitions off of the node. "
                    "This property applies only when "
                    "partition_autobalancing_mode is set to `continuous`.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      80,
      {.min = 5, .max = 100})
  , partition_autobalancing_tick_interval_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "partition_autobalancing_tick_interval_ms",
            .desc = "Partition autobalancer tick interval.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      30s)
  , partition_autobalancing_movement_batch_size_bytes(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "partition_autobalancing_movement_batch_size_bytes",
            .desc = "Total size of partitions that autobalancer is going to "
                    "move in one batch (deprecated, use "
                    "partition_autobalancing_concurrent_moves to limit the "
                    "autobalancer concurrency)",
            .needs_restart = needs_restart::no,
            .visibility = visibility::deprecated,
          };
      }),
      5_GiB)
  , partition_autobalancing_concurrent_moves(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "partition_autobalancing_concurrent_moves",
            .desc = "Number of partitions that can be reassigned at once.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      50)
  , partition_autobalancing_tick_moves_drop_threshold(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "partition_autobalancing_tick_moves_drop_threshold",
            .desc = "If the number of scheduled tick moves drops by this "
                    "ratio, a new tick is scheduled immediately. Valid values "
                    "are (0, 1]. For example, with a value of 0.2 and 100 "
                    "scheduled moves in a tick, a new tick is scheduled when "
                    "the in-progress moves are fewer than 80.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      0.2,
      &validate_0_to_1_ratio)
  , partition_autobalancing_min_size_threshold(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "partition_autobalancing_min_size_threshold",
            .desc
            = "Minimum size of partition that is going to be prioritized when "
              "rebalancing a cluster due to the disk size threshold being "
              "breached. This value is calculated automatically by default.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      std::nullopt)
  , partition_autobalancing_topic_aware(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "partition_autobalancing_topic_aware",
            .desc = "If `true`, Redpanda prioritizes balancing a topic’s "
                    "partition replica count evenly across all brokers while "
                    "it’s balancing the cluster’s overall partition count. "
                    "Because different topics in a cluster can have vastly "
                    "different load profiles, this better distributes the "
                    "workload of the most heavily-used topics evenly across "
                    "brokers.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      true)
  , enable_leader_balancer(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "enable_leader_balancer",
            .desc = "Enable automatic leadership rebalancing.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      true)
  , leader_balancer_mode(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "leader_balancer_mode",
            .desc = "Mode of the leader balancer optimization strategy. "
                    "`calibrated` uses a heuristic that balances leaders based "
                    "on replica counts per shard. `random` randomly moves "
                    "leaders to reduce load on heavily-loaded shards. Legacy "
                    "values `greedy_balanced_shards` and "
                    "`random_hill_climbing` are treated as `calibrated`.",
            .needs_restart = needs_restart::no,
            .example = model::leader_balancer_mode_to_string(
              model::leader_balancer_mode::calibrated),
            .visibility = visibility::user,
          };
      }),
      model::leader_balancer_mode::calibrated,
      {
        model::leader_balancer_mode::calibrated,
        model::leader_balancer_mode::random,
        model::leader_balancer_mode::greedy,
      })
  , leader_balancer_idle_timeout(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "leader_balancer_idle_timeout",
            .desc = "Leadership rebalancing idle timeout.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      2min)
  , leader_balancer_mute_timeout(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "leader_balancer_mute_timeout",
            .desc = "Leadership rebalancing mute timeout.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      5min)
  , leader_balancer_node_mute_timeout(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "leader_balancer_mute_timeout",
            .desc = "Leadership rebalancing node mute timeout.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      20s)
  , leader_balancer_transfer_limit_per_shard(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "leader_balancer_transfer_limit_per_shard",
            .desc = "Per shard limit for in-progress leadership transfers.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      512,
      {.min = 1, .max = 2048})
  , default_leaders_preference(
      *this,
      [](const config::leaders_preference& v) {
          return v != config::leaders_preference{};
      },
      static_metadata([] {
          return base_property::metadata{
            .name = "default_leaders_preference",
            .desc = "Default settings for preferred location of topic "
                    "partition leaders. It can be either \"none\" (no "
                    "preference), or \"racks:<rack1>,<rack2>,...\" (prefer "
                    "brokers with rack id from the list).",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      config::leaders_preference{})
  , core_balancing_on_core_count_change(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "core_balancing_on_core_count_change",
            .desc = "If set to `true`, and if after a restart the number of "
                    "cores changes, Redpanda will move partitions between "
                    "cores to maintain balanced partition distribution.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      true)
  , core_balancing_continuous(
      *this,
      true,
      false,
      static_metadata([] {
          return base_property::metadata{
            .name = "core_balancing_continuous",
            .desc = "If set to `true`, move partitions between cores in "
                    "runtime to maintain balanced partition distribution.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      true,
      property<bool>::noop_validator,
      legacy_default<bool>{false, legacy_version{16}})
  , core_balancing_debounce_timeout(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "core_balancing_debounce_timeout",
            .desc = "Interval, in milliseconds, between trigger and invocation "
                    "of core balancing.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      10s)
  , internal_topic_replication_factor(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "internal_topic_replication_factor",
            .desc = "Target replication factor for internal topics.",
            .visibility = visibility::user,
          };
      }),
      3)
  , health_manager_tick_interval(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "health_manager_tick_interval",
            .desc = "How often the health manager runs.",
            .visibility = visibility::tunable,
          };
      }),
      3min)
  , health_monitor_tick_interval(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "health_monitor_tick_interval",
            .desc = "How often health monitor refresh cluster state",
            .needs_restart = needs_restart::no,
            .visibility = visibility::deprecated,
          };
      }),
      10s)
  , health_monitor_max_metadata_age(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "health_monitor_max_metadata_age",
            .desc
            = "Maximum age of the metadata cached in the health monitor of a "
              "non-controller broker.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      10s)
  , storage_space_alert_free_threshold_percent(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "storage_space_alert_free_threshold_percent",
            .desc = "Threshold of minimum percent free space before setting "
                    "storage space alert.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      5,
      {.min = 0, .max = 50})
  , storage_space_alert_free_threshold_bytes(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "storage_space_alert_free_threshold_bytes",
            .desc = "Threshold of minimum bytes free space before setting "
                    "storage space alert.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      0,
      {.min = 0})
  , storage_min_free_bytes(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "storage_min_free_bytes",
            .desc = "Threshold of minimum bytes free space before rejecting "
                    "producers.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      5_GiB,
      {.min = 10_MiB})
  , storage_strict_data_init(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "storage_strict_data_init",
            .desc = "Requires that an empty file named `.redpanda_data_dir` be "
                    "present in the broker configuration `data_directory`. If "
                    "set to `true`, Redpanda will refuse to start if the file "
                    "is not found in the data directory.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      false)
  , alive_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "alive_timeout_ms",
            .desc = "The amount of time since the last broker status "
                    "heartbeat. After this time, a broker is considered "
                    "offline and not alive.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      5s)
  , memory_abort_on_alloc_failure(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "memory_abort_on_alloc_failure",
            .desc = "If `true`, the Redpanda process will terminate "
                    "immediately when an allocation cannot be satisfied due to "
                    "memory exhaustion. If false, an exception is thrown.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      true)
  , sampled_memory_profile(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "memory_enable_memory_sampling",
            .desc
            = "When `true`, memory allocations are sampled and tracked. A "
              "sampled live set of allocations can then be retrieved from the "
              "Admin API. Additionally, Redpanda will periodically log the "
              "top-n allocation sites.",
            // Enabling/Disabling this dynamically doesn't make much sense as
            // for the
            // memory profile to be meaningful you'll want to have this on from
            // the beginning. However, we still provide the option to be able to
            // disable it dynamically in case something goes wrong
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      true)
  , enable_metrics_reporter(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "enable_metrics_reporter",
            .desc = "Enable the cluster metrics reporter. If `true`, the "
                    "metrics reporter collects and exports to Redpanda Data a "
                    "set of customer usage metrics at the interval set by "
                    "`metrics_reporter_report_interval`. The cluster metrics "
                    "of the metrics reporter are different from the monitoring "
                    "metrics. * The metrics reporter exports customer usage "
                    "metrics for consumption by Redpanda Data.* Monitoring "
                    "metrics are exported for consumption by Redpanda users.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      true)
  , metrics_reporter_tick_interval(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "metrics_reporter_tick_interval",
            .desc = "Cluster metrics reporter tick interval.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      1min)
  , metrics_reporter_report_interval(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "metrics_reporter_report_interval",
            .desc = "Cluster metrics reporter report interval.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      24h)
  , metrics_reporter_url(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "metrics_reporter_url",
            .desc = "URL of the cluster metrics reporter.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      "https://m.rp.vectorized.io/v2")
  , features_auto_enable(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "features_auto_enable",
            .desc = "Whether features whose `available_policy` is `always` or "
                    "`new_clusters_only` are auto-activated by the controller "
                    "after the cluster active logical version reaches their "
                    "required version. When false, the cluster active version "
                    "still advances normally, but each such feature must be "
                    "activated explicitly via the Admin API. Does not affect "
                    "features with `available_policy::explicit_only`, which "
                    "always require explicit activation.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      true)
  , features_auto_finalization(
      *this,
      false, /* restricted value: license required to disable */
      static_metadata([] {
          return base_property::metadata{
            .name = "features_auto_finalization",
            .desc = "Whether the cluster active logical version is advanced "
                    "automatically once all nodes have been upgraded (true), "
                    "or only in response to an explicit request via the Admin "
                    "API (false). When false, the cluster remains able to "
                    "downgrade to the previous version until finalization is "
                    "requested. Setting this to false is an Enterprise feature "
                    "and requires a valid license. Note: if upgrade was "
                    "performed with this set to false and the cluster is ready "
                    "to finalize, flipping this to true does not reliably "
                    "trigger finalization. Leave this set to false and use the "
                    "Admin API to finalize; once the upgrade is complete this "
                    "can be set back to true to restore automatic finalization "
                    "for future upgrades.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      true)
  , enable_rack_awareness(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "enable_rack_awareness",
            .desc = "Enable rack-aware replica assignment.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      false)
  , node_status_interval(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "node_status_interval",
            .desc = "Time interval between two node status messages. Node "
                    "status messages establish liveness status outside of the "
                    "Raft protocol.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      100ms)
  , node_status_reconnect_max_backoff_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "node_status_reconnect_max_backoff_ms",
            .desc = "Maximum backoff (in milliseconds) to reconnect to an "
                    "unresponsive peer during node status liveness checks.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      15s)
  , enable_controller_log_rate_limiting(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "enable_controller_log_rate_limiting",
            .desc = "Limits the write rate for the controller log.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      false)
  , rps_limit_topic_operations(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "rps_limit_topic_operations",
            .desc = "Rate limit for controller topic operations.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      1000)
  , controller_log_accummulation_rps_capacity_topic_operations(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name
            = "controller_log_accummulation_rps_capacity_topic_operations",
            .desc = "Maximum capacity of rate limit accumulationin controller "
                    "topic operations limit",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      std::nullopt)
  , rps_limit_acls_and_users_operations(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "rps_limit_acls_and_users_operations",
            .desc = "Rate limit for controller ACLs and user's operations.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      1000)
  , controller_log_accummulation_rps_capacity_acls_and_users_operations(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "controller_log_accummulation_rps_capacity_acls_and_users_"
                    "operations",
            .desc = "Maximum capacity of rate limit accumulation in controller "
                    "ACLs and users operations limit.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      std::nullopt)
  , rps_limit_node_management_operations(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "rps_limit_node_management_operations",
            .desc = "Rate limit for controller node management operations.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      1000)
  , controller_log_accummulation_rps_capacity_node_management_operations(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "controller_log_accummulation_rps_capacity_node_management_"
                    "operations",
            .desc
            = "Maximum capacity of rate limit accumulation in controller node "
              "management operations limit.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      std::nullopt)
  , rps_limit_move_operations(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "rps_limit_move_operations",
            .desc = "Rate limit for controller move operations.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      1000)
  , controller_log_accummulation_rps_capacity_move_operations(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "controller_log_accummulation_rps_capacity_move_operations",
            .desc
            = "Maximum capacity of rate limit accumulation in controller move "
              "operations limit.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      std::nullopt)
  , rps_limit_configuration_operations(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "rps_limit_configuration_operations",
            .desc = "Rate limit for controller configuration operations.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      1000)
  , controller_log_accummulation_rps_capacity_configuration_operations(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "controller_log_accummulation_rps_capacity_configuration_"
                    "operations",
            .desc = "Maximum capacity of rate limit accumulation in controller "
                    "configuration operations limit.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      std::nullopt)
  , kafka_throughput_limit_node_in_bps(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_throughput_limit_node_in_bps",
            .desc = "The maximum rate of all ingress Kafka API traffic for a "
                    "node. Includes all Kafka API traffic (requests, "
                    "responses, headers, fetched data, produced data, etc.). "
                    "If `null`, the property is disabled, and traffic is not "
                    "limited.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      std::nullopt,
      {.min = 1})
  , kafka_throughput_limit_node_out_bps(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_throughput_limit_node_out_bps",
            .desc = "The maximum rate of all egress Kafka traffic for a node. "
                    "Includes all Kafka API traffic (requests, responses, "
                    "headers, fetched data, produced data, etc.). If `null`, "
                    "the property is disabled, and traffic is not limited.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      std::nullopt,
      {.min = 1})
  , kafka_throughput_replenish_threshold(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_throughput_replenish_threshold",
            .desc
            = "Threshold for refilling the token bucket as part of enforcing "
              "throughput limits. This threshold is evaluated with each "
              "request for data. When the number of tokens to replenish "
              "exceeds this threshold, then tokens are added to the token "
              "bucket. This ensures that the atomic is not being updated for "
              "the token count with each request. The range for this threshold "
              "is automatically clamped to the corresponding throughput limit "
              "for ingress and egress.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      std::nullopt,
      {.min = 1})
  , kafka_throughput_controlled_api_keys(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_throughput_controlled_api_keys",
            .desc = "List of Kafka API keys that are subject to cluster-wide "
                    "and node-wide throughput limit control.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      {"produce", "fetch"})
  , kafka_throughput_control(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_throughput_control",
            .desc = "List of throughput control groups that define exclusions "
                    "from node-wide throughput limits. Clients excluded from "
                    "node-wide throughput limits are still potentially subject "
                    "to client-specific throughput limits. For more "
                    "information see "
                    "https://docs.redpanda.com/current/reference/properties/"
                    "cluster-properties/#kafka_throughput_control.",
            .needs_restart = needs_restart::no,
            .example
            = R"([{'name': 'first_group','client_id': 'client1'}, {'client_id': 'consumer-\d+'}, {'name': 'catch all'}])",
            .visibility = visibility::user,
          };
      }),
      {},
      [](auto& v) {
          return validate_throughput_control_groups(v.cbegin(), v.cend());
      })
  , node_isolation_heartbeat_timeout(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "node_isolation_heartbeat_timeout",
            .desc = "How long after the last heartbeat request a node will "
                    "wait before considering itself to be isolated.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      3000,
      {.min = 100, .max = 10000})
  , controller_snapshot_max_age_sec(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "controller_snapshot_max_age_sec",
            .desc = "Maximum amount of time before Redpanda attempts to create "
                    "a controller snapshot after a new controller command "
                    "appears.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      60s)
  , legacy_permit_unsafe_log_operation(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "legacy_permit_unsafe_log_operation",
            .desc = "Flag to enable a Redpanda cluster operator to use unsafe "
                    "control characters within strings, such as consumer group "
                    "names or user names. This flag applies only for Redpanda "
                    "clusters that were originally on version 23.1 or earlier "
                    "and have been upgraded to version 23.2 or later. Starting "
                    "in version 23.2, newly-created Redpanda clusters ignore "
                    "this property.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      true)
  , legacy_unsafe_log_warning_interval_sec(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "legacy_unsafe_log_warning_interval_sec",
            .desc = "Period at which to log a warning about using unsafe "
                    "strings containing control characters. If unsafe strings "
                    "are permitted by `legacy_permit_unsafe_log_operation`, a "
                    "warning will be logged at an interval specified by this "
                    "property.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      300s)
  , enable_schema_id_validation(
      *this,
      std::vector<pandaproxy::schema_registry::schema_id_validation_mode>{
        pandaproxy::schema_registry::schema_id_validation_mode::compat,
        pandaproxy::schema_registry::schema_id_validation_mode::redpanda,
      },
      static_metadata([] {
          return base_property::metadata{
            .name = "enable_schema_id_validation",
            .desc = "Mode to enable server-side schema ID validation. Accepted "
                    "Values: * `none`: Schema validation is disabled (no "
                    "schema ID checks are done). Associated topic properties "
                    "cannot be modified. * `redpanda`: Schema validation is "
                    "enabled. Only Redpanda topic properties are accepted. * "
                    "`compat`: Schema validation is enabled. Both Redpanda and "
                    "compatible topic properties are accepted.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      pandaproxy::schema_registry::schema_id_validation_mode::none,
      std::vector<pandaproxy::schema_registry::schema_id_validation_mode>{
        pandaproxy::schema_registry::schema_id_validation_mode::none,
        pandaproxy::schema_registry::schema_id_validation_mode::redpanda,
        pandaproxy::schema_registry::schema_id_validation_mode::compat})
  , kafka_schema_id_validation_cache_capacity(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_schema_id_validation_cache_capacity",
            .desc
            = "Per-shard capacity of the cache for validating schema IDs.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      128)
  , schema_registry_enable_authorization(
      *this,
      true,
      static_metadata([] {
          return base_property::metadata{
            .name = "schema_registry_enable_authorization",
            .desc
            = "Enable ACL-based authorization for Schema Registry requests. "
              "When true, uses ACL-based authorization instead of the default "
              "public/user/superuser authorization model. When false, uses the "
              "default authorization model. Requires authentication to be "
              "enabled via schema_registry_api.authn_method.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      false)
  , schema_registry_always_normalize(
      *this,
      static_metadata([] {
          static constexpr std::string_view _aliases[] = {
            "schema_registry_normalize_on_startup"};
          return base_property::metadata{
            .name = "schema_registry_always_normalize",
            .desc = "Always normalize schemas. If set, this overrides the "
                    "normalize parameter in API requests.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
            .aliases = _aliases,
          };
      }),
      false)
  , schema_registry_avro_use_named_references(*this, static_metadata([] {
      return base_property::metadata{
        .name = "schema_registry_avro_use_named_references",
        .visibility = visibility::deprecated,
      };
  }))
  , schema_registry_enable_qualified_subjects(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "schema_registry_enable_qualified_subjects",
            .desc
            = "Enable parsing of qualified subject syntax (:.context:subject). "
              "When true, qualified syntax is parsed to extract context and "
              "subject. When false, subjects are treated literally, as "
              "subjects in the default context.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
          };
      }),
      true)
  , pp_sr_smp_max_non_local_requests(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "pp_sr_smp_max_non_local_requests",
            .desc
            = "Maximum number of Cross-core(Inter-shard communication) "
              "requests pending in HTTP Proxy and Schema Registry seastar::smp "
              "group. (For more details, see the `seastar::smp_service_group` "
              "documentation).",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      std::nullopt)
  , max_in_flight_schema_registry_requests_per_shard(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "max_in_flight_schema_registry_requests_per_shard",
            .desc = "Maximum number of in-flight HTTP requests to Schema "
                    "Registry permitted per shard. Any additional requests "
                    "above this limit will be rejected with a 429 error.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      500,
      {.min = 1})
  , max_in_flight_pandaproxy_requests_per_shard(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "max_in_flight_pandaproxy_requests_per_shard",
            .desc = "Maximum number of in-flight HTTP requests to HTTP Proxy "
                    "permitted per shard. Any additional requests above this "
                    "limit will be rejected with a 429 error.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      500,
      {.min = 1})
  , kafka_memory_share_for_fetch(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "kafka_memory_share_for_fetch",
            .desc
            = "The share of Kafka subsystem memory that can be used for fetch "
              "read buffers, as a fraction of the Kafka subsystem memory "
              "amount.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
          };
      }),
      0.5,
      {.min = 0.0, .max = 1.0})
  , cpu_profiler_enabled(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cpu_profiler_enabled",
            .desc = "Enables CPU profiling for Redpanda.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      false)
  , cpu_profiler_sample_period_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cpu_profiler_sample_period_ms",
            .desc = "The sample period for the CPU profiler.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      100ms,
      {.min = 1ms})
  , rpk_path(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "rpk_path",
            .desc = "Path to RPK binary",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      "/usr/bin/rpk")
  , debug_bundle_storage_dir(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "debug_bundle_storage_dir",
            .desc = "Path to the debug bundle storage directory. Note: "
                    "Changing this path does not clean up existing debug "
                    "bundles. If not set, the debug bundle is stored in the "
                    "Redpanda data directory specified in the redpanda.yaml "
                    "broker configuration file.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      std::nullopt)
  , debug_bundle_auto_removal_seconds(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "debug_bundle_auto_removal_seconds",
            .desc = "If set, how long debug bundles are kept in the debug "
                    "bundle storage directory after they are created. If not "
                    "set, debug bundles are kept indefinitely.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      std::nullopt)
  , oidc_discovery_url(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "oidc_discovery_url",
            .desc = "The URL pointing to the well-known discovery endpoint for "
                    "the OIDC provider.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      "https://auth.prd.cloud.redpanda.com/.well-known/openid-configuration",
      [](const auto& v) -> std::optional<ss::sstring> {
          auto res = security::oidc::parse_url(v);
          if (res.has_error()) {
              return res.error().message();
          }
          return std::nullopt;
      })
  , oidc_http_proxy_url(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "oidc_http_proxy_url",
            .desc
            = "URL of the HTTP forward proxy used for OIDC discovery and JWKS "
              "fetches. Accepts http://host:port or https://host:port. When "
              "set, oidc_discovery_url must use https:// — plaintext OIDC "
              "origins cannot be routed through a forward proxy.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      std::nullopt,
      [](const auto& v) -> std::optional<ss::sstring> {
          if (!v.has_value()) {
              return std::nullopt;
          }
          auto res = security::oidc::parse_url(*v);
          if (res.has_error()) {
              return res.error().message();
          }
          return std::nullopt;
      })
  , oidc_token_audience(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "oidc_token_audience",
            .desc
            = "A string representing the intended recipient of the token.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      "redpanda")
  , oidc_clock_skew_tolerance(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "oidc_clock_skew_tolerance",
            .desc = "The amount of time (in seconds) to allow for when "
                    "validating the expiry claim in the token.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      std::chrono::seconds{} * 30)
  , oidc_principal_mapping(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "oidc_principal_mapping",
            .desc = "Rule for mapping JWT payload claim to a Redpanda user "
                    "principal.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      "$.sub",
      security::oidc::validate_principal_mapping_rule)
  , oidc_keys_refresh_interval(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "oidc_keys_refresh_interval",
            .desc = "The frequency of refreshing the JSON Web Keys (JWKS) used "
                    "to validate access tokens.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      1h)
  , oidc_group_claim_path(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "oidc_group_claim_path",
            .desc = "JSON path to extract groups from the JWT payload.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      "$.groups",
      security::oidc::validate_group_claim_path)
  , nested_group_behavior(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "nested_group_behavior",
            .desc
            = "Behavior for handling nested groups when extracting groups from "
              "authentication tokens. Two options are available - none and "
              "suffix. With none, the group is left alone (e.g. "
              "'/group/child/grandchild'). Suffix will extract the final "
              "component from the nested group (e.g. '/group' -> 'group' and "
              "'/group/child/grandchild' -> 'grandchild').",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      security::oidc::nested_group_behavior::none,
      {security::oidc::nested_group_behavior::none,
       security::oidc::nested_group_behavior::suffix})
  , http_authentication(
      *this,
      "OIDC",
      static_metadata([] {
          return base_property::metadata{
            .name = "http_authentication",
            .desc = "A list of supported HTTP authentication mechanisms. "
                    "Accepted Values: `BASIC`, `OIDC`",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      std::vector<ss::sstring>{"BASIC"},
      validate_http_authn_mechanisms)
  , enable_mpx_extensions(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "enable_mpx_extensions",
            .desc = "Enable Redpanda extensions for MPX.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , virtual_cluster_min_producer_ids(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "virtual_cluster_min_producer_ids",
            .desc = "Minimum number of active producers per virtual cluster.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      std::numeric_limits<uint64_t>::max(),
      {.min = 1})
  , unsafe_enable_consumer_offsets_delete_retention(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "unsafe_enable_consumer_offsets_delete_retention",
            .desc
            = "Enables delete retention of consumer offsets topic. This is an "
              "internal-only configuration and should be enabled only after "
              "consulting with Redpanda support.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
          };
      }),
      false)
  , tls_min_version(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "tls_min_version",
            .desc
            = "The minimum TLS version that Redpanda clusters support. This "
              "property prevents client applications from negotiating a "
              "downgrade to the TLS version when they make a connection to a "
              "Redpanda cluster.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
          };
      }),
      tls_version::v1_2,
      {tls_version::v1_0,
       tls_version::v1_1,
       tls_version::v1_2,
       tls_version::v1_3})
  , tls_enable_renegotiation(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "tls_enable_renegotiation",
            .desc
            = "TLS client-initiated renegotiation is considered unsafe and is "
              "by default disabled. Only re-enable it if you are experiencing "
              "issues with your TLS-enabled client. This option has no effect "
              "on TLSv1.3 connections as client-initiated renegotiation was "
              "removed.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , tls_v1_2_cipher_suites(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "tls_v1_2_cipher_suites",
            .desc = "Specifies the TLS 1.2 cipher suites available for "
                    "external client connections as a colon-separated "
                    "OpenSSL-compatible list. Configure this property to "
                    "support legacy clients.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
          };
      }),
      ss::sstring{net::tls_v1_2_cipher_suites},
      [](const ss::sstring& s) -> std::optional<ss::sstring> {
          if (!validate_tls_v1_2_cipher_suites(s)) {
              return ssx::sformat("Invalid cipher suites: {}", s);
          }
          return std::nullopt;
      })
  , tls_v1_3_cipher_suites(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "tls_v1_3_cipher_suites",
            .desc
            = "Specifies the TLS 1.3 cipher suites available for external "
              "client connections as a colon-separated OpenSSL-compatible "
              "list. Most deployments don't need to modify this setting. "
              "Configure this property only for specific organizational "
              "security policies.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
          };
      }),
      ss::sstring{net::tls_v1_3_cipher_suites},
      [](const ss::sstring& s) -> std::optional<ss::sstring> {
          if (!validate_tls_v1_3_cipher_suites(s)) {
              return ssx::sformat("Invalid cipher suites: {}", s);
          }
          return std::nullopt;
      })
  , iceberg_enabled(
      *this,
      true,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_enabled",
            .desc = "Enables Apache Iceberg integration for storing topic data "
                    "in the Iceberg open table format. Setting iceberg_enabled "
                    "to true activates the feature at the cluster level, but "
                    "each topic must also configure the redpanda.iceberg.mode "
                    "topic-level property to use it.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
          };
      }),
      false)
  , iceberg_catalog_commit_interval_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_catalog_commit_interval_ms",
            .desc = "The frequency at which the Iceberg coordinator commits "
                    "topic files to the catalog. This is the interval between "
                    "commit transactions across all topics monitored by the "
                    "coordinator, not the interval between individual commits.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      std::chrono::milliseconds(1min),
      {.min = std::chrono::milliseconds{10s}})
  , iceberg_latest_schema_cache_ttl_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_latest_schema_cache_ttl_ms",
            .desc = "The TTL for the cache in translation that stores the "
                    "latest schema when using the `value_schema_latest` "
                    "iceberg mode.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      std::chrono::milliseconds(5min),
      {.min = std::chrono::milliseconds{1ms}})
  , iceberg_catalog_base_location(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_catalog_base_location",
            .desc = "Base path for the cloud-storage-object-backed Iceberg "
                    "filesystem catalog. After Iceberg is enabled, do not "
                    "change this value.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
          };
      }),
      "redpanda-iceberg-catalog")
  , iceberg_rest_catalog_base_location(*this, static_metadata([] {
      return base_property::metadata{
        .name = "iceberg_rest_catalog_base_location",
        .desc = "Base URI for the Iceberg REST catalog. If unset, the REST "
                "catalog server determines the location. Some REST catalogs, "
                "like AWS Glue, require the client to set this. After Iceberg "
                "is enabled, do not change this value.",
        .needs_restart = needs_restart::yes,
        .visibility = visibility::user,
      };
  }))
  , datalake_coordinator_snapshot_max_delay_secs(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "datalake_coordinator_snapshot_max_delay_secs",
            .desc = "Maximum amount of time the coordinator waits to snapshot "
                    "after a command appears in the log.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      std::chrono::seconds(15min),
      {.min = 10s})
  , iceberg_catalog_type(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_catalog_type",
            .desc
            = "Iceberg catalog type that Redpanda will use to commit table "
              "metadata updates. Supported types: 'rest', 'object_storage'",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
          };
      }),
      datalake_catalog_type::object_storage,
      {datalake_catalog_type::rest, datalake_catalog_type::object_storage})
  , iceberg_rest_catalog_endpoint(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_rest_catalog_endpoint",
            .desc = "URL of Iceberg REST catalog endpoint",
            .needs_restart = needs_restart::yes,
            .example = "http://hostname:8181",
            .visibility = visibility::user,
          };
      }),
      std::nullopt)
  , iceberg_rest_catalog_client_id(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_rest_catalog_client_id",
            .desc
            = "Iceberg REST catalog user ID. This ID is used to query the "
              "catalog API for the OAuth token. Required if catalog type is "
              "set to `rest` and `iceberg_rest_catalog_authentication_mode` is "
              "set to `oauth2`.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
          };
      }),
      std::nullopt)
  , iceberg_rest_catalog_client_secret(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_rest_catalog_client_secret",
            .desc
            = "Secret to authenticate against Iceberg REST catalog. Required "
              "if catalog type is set to `rest` and "
              "`iceberg_rest_catalog_authentication_mode` is set to `oauth2`.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
            .secret = is_secret::yes,
          };
      }),
      std::nullopt)
  , iceberg_rest_catalog_token(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_rest_catalog_token",
            .desc
            = "Token used to access the REST Iceberg catalog. Required if "
              "`iceberg_rest_catalog_authentication_mode` is set to `bearer`.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
            .secret = is_secret::yes,
          };
      }),
      std::nullopt)
  , iceberg_rest_catalog_request_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_rest_catalog_request_timeout_ms",
            .desc = "Maximum length of time that Redpanda waits for a response "
                    "from the REST catalog before aborting the request",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      10s)
  , iceberg_rest_catalog_trust_file(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_rest_catalog_trust_file",
            .desc = "Path to a file containing a certificate chain to trust "
                    "for the REST Iceberg catalog",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
          };
      }),
      std::nullopt,
      &validate_non_empty_string_opt)
  , iceberg_rest_catalog_trust(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_rest_catalog_trust",
            .desc = "The contents of a certificate chain to trust for the REST "
                    "Iceberg catalog. Takes precedence over "
                    "`iceberg_rest_catalog_trust_file`.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
            .secret = is_secret::yes,
          };
      }),
      std::nullopt,
      &validate_non_empty_string_opt)
  , iceberg_rest_catalog_crl_file(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_rest_catalog_crl_file",
            .desc = "Path to certificate revocation list for "
                    "`iceberg_rest_catalog_trust_file`.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
          };
      }),
      std::nullopt,
      &validate_non_empty_string_opt)
  , iceberg_rest_catalog_crl(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_rest_catalog_crl",
            .desc = "The contents of a certificate revocation list for "
                    "`iceberg_rest_catalog_trust`. Takes precedence over "
                    "`iceberg_rest_catalog_crl_file`.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
            .secret = is_secret::yes,
          };
      }),
      std::nullopt,
      &validate_non_empty_string_opt)
  , iceberg_rest_catalog_warehouse(
      *this,
      static_metadata([] {
          static constexpr std::string_view _aliases[] = {
            "iceberg_rest_catalog_prefix"};
          return base_property::metadata{
            .name = "iceberg_rest_catalog_warehouse",
            .desc
            = "Warehouse to use for the Iceberg REST catalog. Redpanda will "
              "query the catalog for configurations specific to the warehouse, "
              "for example, using it to automatically configure the "
              "appropriate prefix.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
            .aliases = _aliases,
          };
      }),
      std::nullopt,
      &validate_non_empty_string_opt)
  , iceberg_rest_catalog_oauth2_server_uri(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_rest_catalog_oauth2_server_uri",
            .desc = "The OAuth URI used to retrieve access tokens for Iceberg "
                    "catalog authentication. If left undefined, the deprecated "
                    "Iceberg catalog endpoint `/v1/oauth/tokens` is used "
                    "instead.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
          };
      }),
      std::nullopt,
      &validate_non_empty_string_opt)
  , iceberg_rest_catalog_oauth2_scope(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_rest_catalog_oauth2_scope",
            .desc
            = "The OAuth scope used to retrieve access tokens for Iceberg "
              "catalog authentication. Only meaningful when "
              "`iceberg_rest_catalog_authentication_mode` is set to `oauth2`",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
          };
      }),
      "PRINCIPAL_ROLE:ALL")
  , iceberg_rest_catalog_authentication_mode(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_rest_catalog_authentication_mode",
            .desc
            = "The authentication mode for client requests made to the Iceberg "
              "catalog. Choose from: `none`, `bearer`, `oauth2`, and "
              "`aws_sigv4`. In `bearer` mode, the token specified in "
              "`iceberg_rest_catalog_token` is used unconditonally, and no "
              "attempts are made to refresh the token. In `oauth2` mode, the "
              "credentials specified in `iceberg_rest_catalog_client_id` and "
              "`iceberg_rest_catalog_client_secret` are used to obtain a "
              "bearer token from the URI defined by "
              "`iceberg_rest_catalog_oauth2_server_uri`. In `aws_sigv4` mode, "
              "the same AWS credentials used for cloud storage (see "
              "`cloud_storage_region`, `cloud_storage_access_key`, "
              "`cloud_storage_secret_key`, and "
              "`cloud_storage_credentials_source`) are used to sign requests "
              "to AWS Glue catalog with SigV4.In `gcp` mode Redpanda will use "
              "VM metadata for authentication.",
            .needs_restart = needs_restart::yes,
            .example = "none",
            .visibility = visibility::user,
          };
      }),
      datalake_catalog_auth_mode::none,
      {
        datalake_catalog_auth_mode::none,
        datalake_catalog_auth_mode::bearer,
        datalake_catalog_auth_mode::oauth2,
        datalake_catalog_auth_mode::aws_sigv4,
        datalake_catalog_auth_mode::gcp,
      })
  , iceberg_rest_catalog_aws_service_name(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_rest_catalog_aws_service_name",
            .desc = "AWS service name for SigV4 signing when using aws_sigv4 "
                    "authentication mode. Defaults to 'glue' for AWS Glue Data "
                    "Catalog. Can be changed to support other AWS services "
                    "that provide Iceberg REST catalog APIs.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
          };
      }),
      "glue",
      &validate_non_empty_string)
  , iceberg_rest_catalog_aws_access_key(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_rest_catalog_aws_access_key",
            .desc = "AWS access key for Iceberg REST catalog SigV4 "
                    "authentication. If not set, falls back to "
                    "cloud_storage_access_key when using aws_sigv4 "
                    "authentication mode.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
          };
      }),
      std::nullopt,
      &validate_non_empty_string_opt)
  , iceberg_rest_catalog_aws_secret_key(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_rest_catalog_aws_secret_key",
            .desc = "AWS secret key for Iceberg REST catalog SigV4 "
                    "authentication. If not set, falls back to "
                    "cloud_storage_secret_key when using aws_sigv4 "
                    "authentication mode.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
            .secret = is_secret::yes,
          };
      }),
      std::nullopt,
      &validate_non_empty_string_opt)
  , iceberg_rest_catalog_aws_region(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_rest_catalog_aws_region",
            .desc = "AWS region for Iceberg REST catalog SigV4 authentication. "
                    "If not set, falls back to cloud_storage_region when using "
                    "aws_sigv4 authentication mode.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
          };
      }),
      std::nullopt,
      &validate_non_empty_string_opt)
  , iceberg_rest_catalog_aws_credentials_source(
      *this,
      static_metadata([] {
          // Bad original name. The source may not always relate to AWS.
          static constexpr std::string_view _aliases[] = {
            "iceberg_rest_catalog_aws_credentials_source"};
          return base_property::metadata{
            .name = "iceberg_rest_catalog_credentials_source",
            .desc = "Source of AWS credentials for Iceberg REST catalog SigV4 "
                    "authentication. If not set, falls back to "
                    "cloud_storage_credentials_source when using aws_sigv4 "
                    "authentication mode. Accepted values: config_file, "
                    "aws_instance_metadata, sts, gcp_instance_metadata, "
                    "azure_vm_instance_metadata, azure_aks_oidc_federation.",
            .needs_restart = needs_restart::yes,
            .example = "config_file",
            .visibility = visibility::user,
            .aliases = _aliases,
          };
      }),
      std::nullopt,
      {
        model::cloud_credentials_source::config_file,
        model::cloud_credentials_source::aws_instance_metadata,
        model::cloud_credentials_source::sts,
        model::cloud_credentials_source::gcp_instance_metadata,
        model::cloud_credentials_source::azure_aks_oidc_federation,
        model::cloud_credentials_source::azure_vm_instance_metadata,
      })
  , iceberg_rest_catalog_gcp_user_project(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_rest_catalog_gcp_user_project",
            .desc = "The GCP project that is billed for charges associated "
                    "with Iceberg REST Catalog requests.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
          };
      }),
      std::nullopt,
      &validate_non_empty_string_opt)
  , iceberg_rest_catalog_credentials_host(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_rest_catalog_credentials_host",
            .desc
            = "The hostname to connect to for retrieving role based "
              "credentials for the Iceberg REST catalog. Derived from "
              "iceberg_rest_catalog_credentials_source if not set. Only "
              "required when using IAM role based access on AWS; does not "
              "apply to OAuth-based authentication schemes. Independent of "
              "cloud_storage_credentials_host.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
          };
      }),
      std::nullopt,
      &validate_non_empty_string_opt)
  , iceberg_backlog_controller_p_coeff(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_backlog_controller_p_coeff",
            .desc = "Proportional coefficient for the Iceberg backlog "
                    "controller. Number of shares assigned to the datalake "
                    "scheduling group will be proportional to the backlog size "
                    "error.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      0.00001)
  , iceberg_backlog_controller_i_coeff(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_backlog_controller_i_coeff",
            .desc
            = "Integral coefficient for the Iceberg backlog controller. The "
              "error is integrated (accumulated) over time and the aggregated "
              "value contributes to the datalake translation priority with "
              "this coefficient.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      0.005)
  , iceberg_target_backlog_size(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_target_backlog_size",
            .desc
            = "Total size of the datalake translation backlog that the backlog "
              "controller will try to maintain. When a backlog size is larger "
              "than the setpoint a backlog controller will increase the "
              "translation scheduling group priority.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      100_MiB,
      {.min = 0, .max = std::numeric_limits<uint32_t>::max()})
  , iceberg_throttle_backlog_size_ratio(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_throttle_backlog_size_ratio",
            .desc = "Ratio of total backlog size to disk space that triggers "
                    "throttling for Iceberg producers. Set to `null` to "
                    "disable throttling.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      std::nullopt)
  , iceberg_delete(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_delete",
            .desc = "Default value for the redpanda.iceberg.delete topic "
                    "property that determines if the corresponding Iceberg "
                    "table is deleted upon deleting the topic.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      true)
  , iceberg_default_partition_spec(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_default_partition_spec",
            .desc = "Default value for the redpanda.iceberg.partition.spec "
                    "topic property that determines the partition spec for the "
                    "Iceberg table corresponding to the topic. If this "
                    "property is not set and AWS Glue is being used as the "
                    "Iceberg REST catalog, the default value will be "
                    "overridden by an empty partition spec, for compatibility "
                    "with AWS Glue.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      "(hour(redpanda.timestamp))",
      &validate_iceberg_partition_spec)
  , iceberg_invalid_record_action(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_invalid_record_action",
            .desc = "Default value for the "
                    "redpanda.iceberg.invalid.record.action topic property.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      model::iceberg_invalid_record_action::dlq_table,
      {
        model::iceberg_invalid_record_action::drop,
        model::iceberg_invalid_record_action::dlq_table,
      })
  , iceberg_target_lag_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_target_lag_ms",
            .desc = "Default value for the redpanda.iceberg.target.lag.ms "
                    "topic property, which controls how often data in an "
                    "Iceberg table is refreshed with new data from the "
                    "corresponding Redpanda topic. Redpanda attempts to commit "
                    "all the data produced to the topic within the lag target "
                    "in a best effort fashion, subject to resource "
                    "availability.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      std::chrono::milliseconds{1min},
      {.min = std::chrono::milliseconds{10s},
       .max = serde::max_serializable_ms})
  , iceberg_disable_snapshot_tagging(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_disable_snapshot_tagging",
            .desc
            = "Whether to disable tagging of Iceberg snapshots. These tags are "
              "used to ensure that the snapshots that Redpanda writes are "
              "retained during snapshot removal, which in turn, helps Redpanda "
              "ensure exactly once delivery of records. Disabling tags is "
              "therefore not recommended, but may be useful if the Iceberg "
              "catalog does not support tags.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      false)
  , iceberg_disable_automatic_snapshot_expiry(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_disable_automatic_snapshot_expiry",
            .desc = "Whether to disable automatic Iceberg snapshot expiry. "
                    "This may be useful if the Iceberg catalog expects to "
                    "perform snapshot expiry on its own.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      false)
  , iceberg_topic_name_dot_replacement(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_topic_name_dot_replacement",
            .desc = "Optional replacement string for dots in topic names when "
                    "deriving Iceberg table names, useful when downstream "
                    "systems do not permit dots in table names. The "
                    "replacement string cannot contain dots. Be careful to "
                    "avoid table name collisions caused by the replacement.If "
                    "an Iceberg topic with dots in the name exists in the "
                    "cluster, the value of this property should not be "
                    "changed.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      std::nullopt,
      &validate_iceberg_topic_name_dot_replacement)
  , iceberg_dlq_table_suffix(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_dlq_table_suffix",
            .desc = "Suffix appended to the Iceberg table name for the "
                    "dead-letter queue (DLQ) table that stores invalid records "
                    "when the invalid record action is set to `dlq_table`. "
                    "Should be chosen in a way that avoids name collisions "
                    "with other Iceberg tables. Important for catalogs which "
                    "do not support ~ (tilde) in table names. Should not be "
                    "changed after any DLQ tables have been created.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      "~dlq",
      &validate_non_empty_string)
  , iceberg_default_catalog_namespace(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "iceberg_default_catalog_namespace",
            .desc = "The default namespace (database name) for Iceberg tables. "
                    "All tables created by Redpanda will be placed in this "
                    "namespace within the Iceberg catalog. Supports nested "
                    "namespaces as an array of strings. IMPORTANT: This value "
                    "must be configured before enabling Iceberg and must not "
                    "be changed afterward. Changing it will cause Redpanda to "
                    "lose track of existing tables.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
          };
      }),
      {"redpanda"},
      &validate_iceberg_default_catalog_namespace)
  , enable_host_metrics(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "enable_host_metrics",
            .desc = "Enable exporting of some host metrics like "
                    "/proc/diskstats, /proc/snmp and /proc/net/netstat",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      true)
  , datalake_scheduler_block_size_bytes(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "datalake_scheduler_block_size_bytes",
            .desc = "Size, in bytes, of each memory block reserved for record "
                    "translation, as tracked by the datalake scheduler.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      4_MiB,
      {.min = 1_MiB, .max = 8_MiB})
  , datalake_scheduler_max_concurrent_translations(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "datalake_scheduler_max_concurrent_translations",
            .desc = "The maximum number of translations that the datalake "
                    "scheduler will allow to run at a given time. If a "
                    "translation is requested but the number of running "
                    "translations exceeds this value, the request will be put "
                    "to sleep temporarily, polling until capacity becomes "
                    "available.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      4,
      {.min = 1, .max = 8})
  , datalake_scheduler_time_slice_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "datalake_scheduler_time_slice_ms",
            .desc = "Time, in milliseconds, for a datalake translation as "
                    "scheduled by the datalake scheduler. After a translation "
                    "is scheduled, it will run until either a) the time "
                    "specified has elapsed or b) all pending records on its "
                    "source partition have been translated.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      30s,
      {.min = 1s, .max = 60s})
  , datalake_translator_flush_bytes(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "datalake_translator_flush_bytes",
            .desc = "Size, in bytes, of the amount of per translator data that "
                    "may be flushed to disk before the translator will upload "
                    "and remove its current on disk data.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      32_MiB,
      {.min = 1_MiB})
  , datalake_disk_space_monitor_enable(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "datalake_disk_space_monitor_enable",
            .desc = "Option to explicitly disable enforcement of datalake disk "
                    "space usage",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      true)
  , datalake_scratch_space_size_bytes(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "datalake_scratch_space_size_bytes",
            .desc = "Size, in bytes, of the amount of scratch space datalake "
                    "should use.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      5_GiB)
  , datalake_scratch_space_soft_limit_size_percent(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "datalake_scratch_space_soft_limit_size_percent",
            .desc
            = "Size of the scratch space datalake soft limit expressed as a "
              "percentage of the datalake_scratch_space_size_bytes "
              "configuration value.",
            .needs_restart = needs_restart::no,
            .example = "80.0",
            .visibility = visibility::user,
          };
      }),
      80.0,
      {.min = 0.0, .max = 100.0})
  , datalake_disk_usage_overage_coeff(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "datalake_disk_usage_overage_coeff",
            .desc = "The datalake disk usage monitor reclaims the overage "
                    "multiplied by this this coefficient to compensate for "
                    "data that is written during the idle period between "
                    "control loop invocations.",
            .needs_restart = needs_restart::no,
            .example = "1.8",
            .visibility = visibility::tunable,
          };
      }),
      2.0)
  , datalake_scheduler_disk_reservation_block_size(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "datalake_scheduler_disk_reservation_block_size",
            .desc = "The size, in bytes, of the block of disk reservation that "
                    "the datalake manager will assign to each datalake "
                    "scheduler when it runs out of local reservation.",
            .needs_restart = needs_restart::no,
            .example = "10000000",
            .visibility = visibility::tunable,
          };
      }),
      50_MiB,
      {.min = 1_MiB})
  , consumer_offsets_topic_batch_cache_enabled(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "consumer_offsets_topic_batch_cache_enabled",
            .desc = "This property lets you enable the batch cache for the "
                    "consumer offsets topic. By default, the cache for "
                    "consumer offsets topic is disabled. Changing this "
                    "property is not recommended in production systems, as it "
                    "may affect performance. The change is applied only after "
                    "the restart.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , enable_shadow_linking(
      *this,
      true,
      static_metadata([] {
          return base_property::metadata{
            .name = "enable_shadow_linking",
            .desc = "Enable creating Shadow Links from this cluster to a "
                    "remote source cluster for data replication.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      false)
  , shadow_link_failover_batch_size(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "shadow_link_failover_batch_size",
            .desc
            = "Maximum number of mirror topics to include in a single batched "
              "failover controller command.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      1000,
      {.min = 1})
  , internal_rpc_request_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "internal_rpc_request_timeout_ms",
            .desc = "Default timeout for RPC requests between Redpanda nodes.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      10s)
  , cloud_topics_enabled(
      *this,
      true,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_enabled",
            .desc = "Enable cloud topics.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
          };
      }),
      false)
  , cloud_topics_produce_batching_size_threshold(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_produce_batching_size_threshold",
            .desc
            = "The size limit for the object size in cloud topics. When the "
              "amount of data on a shard reaches this limit, an upload is "
              "triggered.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      4_MiB)
  , cloud_topics_produce_upload_interval(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_produce_upload_interval",
            .desc = "Time interval after which the upload is triggered.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      250ms)
  , cloud_topics_produce_cardinality_threshold(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_produce_cardinality_threshold",
            .desc
            = "Threshold for the object cardinality in cloud topics. When the "
              "number of partitions in waiting for the upload reach this "
              "limit, an upload is triggered.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      1000)
  , cloud_topics_disable_reconciliation_loop(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_disable_reconciliation_loop",
            .desc
            = "Disables the cloud topics reconciliation loop. Disabling the "
              "loop can negatively impact performance and stability of the "
              "cluster.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , cloud_topics_reconciliation_min_interval(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_reconciliation_min_interval",
            .desc
            = "Minimum reconciliation interval for adaptive scheduling. The "
              "reconciler will not run more frequently than this.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      250ms)
  , cloud_topics_reconciliation_max_interval(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_reconciliation_max_interval",
            .desc = "Maximum reconciliation interval for adaptive scheduling.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      10s)
  , cloud_topics_reconciliation_target_fill_ratio(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_reconciliation_target_fill_ratio",
            .desc = "Target fill ratio for L1 objects. The reconciler adapts "
                    "its interval to produce objects at approximately this "
                    "fill level (0.0 to 1.0).",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      0.8,
      validate_0_to_1_ratio)
  , cloud_topics_reconciliation_speedup_blend(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_reconciliation_speedup_blend",
            .desc = "Blend factor for speeding up reconciliation (0.0 to 1.0). "
                    "Higher values mean reconciliation increases its frequency "
                    "faster when trying to find a frequency that produces "
                    "well-sized objects.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      0.9,
      validate_0_to_1_ratio)
  , cloud_topics_reconciliation_slowdown_blend(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_reconciliation_slowdown_blend",
            .desc = "Blend factor for slowing down reconciliation (0.0 to "
                    "1.0). Higher values mean reconciliation lowers its "
                    "frequency faster when trying to find a frequency that "
                    "produces well-sized objects. Generally this should be "
                    "lower than the speedup blend, because reconciliation has "
                    "less opportunities to adapt its frequency when it runs "
                    "less frequently.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      0.4,
      validate_0_to_1_ratio)
  , cloud_topics_reconciliation_max_object_size(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_reconciliation_max_object_size",
            .desc = "Maximum size in bytes for L1 objects produced by the "
                    "reconciler. With the default target fill ratio of 0.8, "
                    "this gives an effective target object size of 64 MiB.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      80_MiB)
  , cloud_topics_upload_part_size(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_upload_part_size",
            .desc
            = "The part size in bytes used for multipart uploads. The minimum "
              "of 5 MiB is the smallest non-terminal part size allowed by "
              "cloud object storage providers.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      16_MiB,
      {.min = 5_MiB, .max = 128_MiB})
  , cloud_topics_reconciliation_parallelism(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_reconciliation_parallelism",
            .desc = "Maximum number, per shard, of concurrent objects built by "
                    "reconciliation",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      8,
      {.min = size_t{1}, .max = size_t{64}})
  , cloud_topics_allow_materialization_failure(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_allow_materialization_failure",
            .desc = "When enabled, the reconciler tolerates missing L0 extent "
                    "objects (404 errors) during materialization. Failed "
                    "extents are skipped, producing L1 state with empty offset "
                    "ranges where deleted data was. Use this to recover "
                    "partitions after accidental deletion of live extent "
                    "objects.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , cloud_topics_compaction_max_object_size(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_compaction_max_object_size",
            .desc
            = "Maximum size in bytes for L1 objects produced by cloud topics "
              "compaction.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      128_MiB)
  , cloud_topics_l1_indexing_interval(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_indexing_interval",
            .desc = "The byte interval at which index entries are created "
                    "within long term storage objects for cloud topics. Index "
                    "entries are stored in the object metadata and enable "
                    "efficient seeking by offset or timestamp within a "
                    "partition. Lower values produce more index entries "
                    "(better seek granularity) at the cost of a larger footer.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      4_MiB)
  , cloud_topics_compaction_interval_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_compaction_interval_ms",
            .desc
            = "How often to trigger background compaction for cloud topics.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      30s)
  , cloud_topics_compaction_key_map_memory(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_compaction_key_map_memory",
            .desc = "Maximum number of bytes that may be used on each shard by "
                    "cloud topics compaction key-offset maps.",
            .needs_restart = needs_restart::yes,
            .example = "134217728",
            .visibility = visibility::tunable,
          };
      }),
      128_MiB,
      {.min = 16_MiB, .max = 100_GiB})
  , cloud_topics_long_term_garbage_collection_interval(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_long_term_garbage_collection_interval",
            .desc
            = "Time interval after which data is garbage collected from long "
              "term storage.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      5min)
  , cloud_topics_long_term_flush_interval(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_long_term_flush_interval",
            .desc = "Time interval at which long term storage metadata is "
                    "flushed to object storage.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      10min)
  , cloud_topics_epoch_service_epoch_increment_interval(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_epoch_service_epoch_increment_interval",
            .desc = "The interval at which the cluster epoch is incremented.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      10min)
  , cloud_topics_epoch_service_local_epoch_cache_duration(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_epoch_service_local_epoch_cache_duration",
            .desc = "The local cache duration of a cluster wide epoch.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      1min)
  , cloud_topics_epoch_service_max_same_epoch_duration(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_epoch_service_max_same_epoch_duration",
            .desc
            = "The duration of time that a node can use the exact same epoch.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      24 * 60min)
  , cloud_topics_short_term_gc_minimum_object_age(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_short_term_gc_minimum_object_age",
            .desc = "The minimum age of an L0 object before it becomes "
                    "eligible for garbage collection.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      12h)
  , cloud_topics_short_term_gc_interval(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_short_term_gc_interval",
            .desc = "The interval between invocations of the L0 garbage "
                    "collection work loop when progress is being made.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      10s)
  , cloud_topics_short_term_gc_backoff_interval(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_short_term_gc_backoff_interval",
            .desc = "The interval between invocations of the L0 garbage "
                    "collection work loop when no progress is being made or "
                    "errors are occurring.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      1min)
  , cloud_topics_gc_health_check_interval(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_gc_health_check_interval",
            .desc = "The interval at which the L0 garbage collector checks "
                    "cluster health. GC will not proceed while the cluster is "
                    "unhealthy.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      10s)
  , cloud_topics_metastore_replication_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_metastore_replication_timeout_ms",
            .desc = "Timeout for L1 metastore Raft replication and waiting for "
                    "the STM to apply the replicated write batch.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      30s)
  , cloud_topics_metastore_lsm_apply_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_metastore_lsm_apply_timeout_ms",
            .desc
            = "Timeout for applying a replicated write batch to the local LSM "
              "database. This may take longer than usual when L0 compaction is "
              "behind and writes are being throttled.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      5min)
  , cloud_topics_parallel_fetch_enabled(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_parallel_fetch_enabled",
            .desc = "Enable parallel fetching in cloud topics. This mechanism "
                    "improves the throughput by allowing the broker to "
                    "download data needed by the fetch request using multiple "
                    "shards.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
          };
      }),
      true)
  , cloud_topics_fetch_debounce_enabled(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_fetch_debounce_enabled",
            .desc = "Enables fetch debouncing in cloud topics. This mechanism "
                    "guarantees that the broker fetches every object only once "
                    "improving the performance and lowering the cost.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::user,
          };
      }),
      true)
  , cloud_topics_preregistered_object_ttl(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_preregistered_object_ttl",
            .desc = "Time-to-live for pre-registered L1 objects before they "
                    "are expired.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      1h)
  , cloud_topics_long_term_file_deletion_delay(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_long_term_file_deletion_delay",
            .desc = "Delay before deleting stale long term files, allowing "
                    "concurrent readers (e.g. read replica topics) to finish "
                    "reading them before removal.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      1h)
  , cloud_topics_num_metastore_partitions(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_num_metastore_partitions",
            .desc
            = "Number of partitions for the cloud topics metastore topic, used "
              "to spread metastore load across the cluster. Higher values "
              "allow more parallel metadata operations but reduce the amount "
              "of work each partition can batch together. Only takes effect "
              "when the metastore topic is first created.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      3,
      {.min = 1})
  , cloud_topics_produce_write_inflight_limit(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_produce_write_inflight_limit",
            .desc = "Maximum number of in-flight write requests per shard in "
                    "the cloud topics write pipeline. Requests that exceed "
                    "this limit are queued until a slot becomes available.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      1024,
      {.min = 1})
  , cloud_topics_produce_no_pid_concurrency(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_produce_no_pid_concurrency",
            .desc = "Maximum number of concurrent raft replication requests "
                    "for producers without a producer ID (idempotency "
                    "disabled). Limits how many no-PID writes can proceed past "
                    "the producer queue into raft simultaneously.",
            .needs_restart = needs_restart::yes,
            .visibility = visibility::tunable,
          };
      }),
      32,
      {.min = 1})
  , cloud_topics_l1_reader_cache_eviction_timeout_ms(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_l1_reader_cache_eviction_timeout_ms",
            .desc
            = "Time after which idle L1 readers are evicted from the per-shard "
              "reader cache.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      60'000ms)
  , cloud_topics_l1_reader_cache_max_size(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "cloud_topics_l1_reader_cache_max_size",
            .desc = "Maximum number of L1 readers cached per shard. When the "
                    "cache exceeds this limit, the oldest idle reader is "
                    "evicted.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      128,
      {.min = 0, .max = 10000})
  , code_hugepages_enabled(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "code_hugepages_enabled",
            .desc = "Map the binary into hugepages",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      false)
  , development_feature_property_testing_only(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name = "development_feature_property_testing_only",
            .desc = "Development feature property for testing only.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::user,
          };
      }),
      false)
  , enable_developmental_unrecoverable_data_corrupting_features(
      *this,
      static_metadata([] {
          return base_property::metadata{
            .name
            = "enable_developmental_unrecoverable_data_corrupting_features",
            .desc
            = "Development features should never be enabled in a production "
              "cluster, or any cluster where stability, data loss, or the "
              "ability to upgrade are a concern. To enable experimental "
              "features, set the value of this configuration option to the "
              "current unix epoch expressed in seconds. The value must be "
              "within one hour of the current time on the broker.Once "
              "experimental features are enabled they cannot be disabled.",
            .needs_restart = needs_restart::no,
            .visibility = visibility::tunable,
          };
      }),
      "",
      // Stateless: looks up the property on the shard-local configuration.
      // Safe because this validator only fires on user-initiated updates,
      // long after the initial construction is complete.
      [](const ss::sstring& v) -> std::optional<ss::sstring> {
          if (config::shard_local_cfg().development_features_enabled()) {
              return fmt::format(
                "Development feature flag cannot be changed once enabled.");
          }

          const auto time_since_epoch
            = std::chrono::system_clock::now().time_since_epoch();

          try {
              const auto key = std::chrono::seconds(
                boost::lexical_cast<int64_t>(v));

              const auto dur = std::chrono::abs(time_since_epoch - key);
              if (dur > std::chrono::hours(1)) {
                  return fmt::format(
                    "Invalid key '{}'. Must be within 1 hour of the current "
                    "unix epoch in seconds.",
                    key.count());
              }
          } catch (const boost::bad_lexical_cast&) {
              return fmt::format("Could not convert '{}' to integer", v);
          }

          return std::nullopt;
      }) {}

configuration::error_map_t configuration::load(const YAML::Node& root_node) {
    if (!root_node["redpanda"]) {
        throw std::invalid_argument("'redpanda' root is required");
    }

    return config_store::read_yaml(root_node["redpanda"]);
}

std::unique_ptr<configuration> make_config() {
    // Constructing `configuration` requires about 90KB of stack space in debug.
    // In some tests this makes us run out of stack space/into stackoverflows as
    // the default stack for ss::thread is 128KiB.
    //
    // Hence we construct the configuration object in a separate thread with
    // increased stack size.
    //
    // Also we want to keep the configuration object of the stack itself as it's
    // even larger (>90KB). Hence, we take the unique_ptr indirection and store
    // it on the heap. This also avoids having to rely on RVO and friends.
    //
    // Note this is above our usual max allocation limit of 128KiB but this
    // isn't much of an issue here as this happens on startup (and also before
    // the warning threshold is set up)
    //
    // Note all of this only happens when running with the reactor active and on
    // a ss::thread.  ss::thread requires the reactor to be inited. This isn't
    // the case in all tests (BOOST_AUTO_TEST_CASE). Further, otherwise we are
    // running on a native posix thread with large stack anyway so this isn't an
    // issue.
    auto make_cfg = []() { return std::make_unique<configuration>(); };
    if (seastar::engine_is_ready() && ss::thread::running_in_thread()) {
        ss::thread_attributes attrs;
        attrs.stack_size = 512_KiB;
        return ss::async(attrs, make_cfg).get();
    } else {
        return make_cfg();
    }
}

configuration& shard_local_cfg() {
    static thread_local std::unique_ptr<configuration> cfg = make_config();
    return *cfg;
}
} // namespace config
