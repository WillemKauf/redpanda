// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/topics/types.h"

#include "base/units.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "container/fragmented_vector.h"
#include "kafka/server/handlers/configs/config_response_utils.h"
#include "kafka/server/handlers/configs/config_utils.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/timestamp.h"
#include "pandaproxy/schema_registry/subject_name_strategy.h"
#include "strings/string_switch.h"
#include "utils/tristate.h"

#include <seastar/core/sstring.hh>

#include <bits/stdint-intn.h>
#include <bits/stdint-uintn.h>

#include <chrono>
#include <cstddef>
#include <limits>
#include <optional>
#include <ratio>
#include <string_view>
#include <vector>

namespace kafka {

template<typename T>
concept CreatableTopicCfg = std::is_same_v<T, creatable_topic_configs>
                            || std::is_same_v<T, createable_topic_config>;

template<typename Container>
concept CreatableTopicCfgContainer = requires(Container c) {
    requires CreatableTopicCfg<typename Container::value_type>;
};

template<CreatableTopicCfgContainer T>
config_map_t make_config_map(const T& config) {
    config_map_t ret;
    ret.reserve(config.size());
    for (const auto& c : config) {
        if (c.value) {
            ret.emplace(c.name, *c.value);
        }
    }
    return ret;
}

config_map_t config_map(const std::vector<createable_topic_config>& config) {
    return make_config_map(config);
}

config_map_t config_map(const std::vector<creatable_topic_configs>& config) {
    return make_config_map(config);
}

// Either parse configuration or return nullopt
template<typename T>
static std::optional<T>
get_config_value(const config_map_t& config, std::string_view key) {
    if (auto it = config.find(key); it != config.end()) {
        return boost::lexical_cast<T>(it->second);
    }
    return std::nullopt;
}

template<class T>
requires std::
  is_same_v<T, std::chrono::duration<typename T::rep, typename T::period>>
  static std::optional<T> get_duration_value(
    const config_map_t& config,
    std::string_view key,
    bool clamp_to_duration_max = false) {
    if (auto it = config.find(key); it != config.end()) {
        auto parsed = boost::lexical_cast<typename T::rep>(it->second);
        // Certain Kafka clients have LONG_MAX duration to represent
        // maximum duration but that overflows during serde serialization
        // to nanos. Clamping to max allowed duration gives the same
        // desired behavior of no timeout without having to fail the request.
        constexpr auto max = std::chrono::duration_cast<T>(
          std::chrono::nanoseconds::max());
        auto v = clamp_to_duration_max ? std::min(parsed, max.count()) : parsed;
        return T{v};
    }
    return std::nullopt;
}

static std::optional<ss::sstring>
get_string_value(const config_map_t& config, std::string_view key) {
    if (auto it = config.find(key); it != config.end()) {
        return it->second;
    }
    return std::nullopt;
}

// Special case for options where Kafka allows -1
// In redpanda the mapping is following
//
// -1 (feature disabled)   =>  tristate.is_disabled() == true;
// no value                =>  tristate.has_value() == false;
// value present           =>  tristate.has_value() == true;

template<typename T>
static tristate<T>
get_tristate_value(const config_map_t& config, std::string_view key) {
    auto v = get_config_value<int64_t>(config, key);
    // no value set
    if (!v) {
        return tristate<T>(std::nullopt);
    }
    // disabled case
    if (v <= 0) {
        return tristate<T>(disable_tristate);
    }
    return tristate<T>(std::make_optional<T>(*v));
}

template<typename T>
static std::optional<T>
get_enum_value(const config_map_t& config, std::string_view key) {
    T ret;
    auto s_opt = get_string_value(config, key);
    if (!s_opt) {
        return std::nullopt;
    }
    auto is = std::istringstream(*s_opt);
    is >> ret;
    if (is.fail()) {
        return std::nullopt;
    }
    return ret;
}

cluster::custom_assignable_topic_configuration
to_cluster_type(const creatable_topic& t) {
    auto cfg = cluster::topic_configuration(
      model::kafka_namespace, t.name, t.num_partitions, t.replication_factor);

    auto config_entries = config_map(t.configs);
    // Parse topic configuration
    cfg.properties.compression = get_config_value<model::compression>(
      config_entries, topic_property_compression);
    cfg.properties.cleanup_policy_bitflags
      = get_config_value<model::cleanup_policy_bitflags>(
        config_entries, topic_property_cleanup_policy);
    cfg.properties.timestamp_type = get_config_value<model::timestamp_type>(
      config_entries, topic_property_timestamp_type);
    cfg.properties.segment_size = get_config_value<size_t>(
      config_entries, topic_property_segment_size);
    cfg.properties.compaction_strategy
      = get_config_value<model::compaction_strategy>(
        config_entries, topic_property_compaction_strategy);
    cfg.properties.retention_bytes = get_tristate_value<size_t>(
      config_entries, topic_property_retention_bytes);
    cfg.properties.retention_duration
      = get_tristate_value<std::chrono::milliseconds>(
        config_entries, topic_property_retention_duration);
    cfg.properties.recovery = get_bool_value(
      config_entries, topic_property_recovery);
    cfg.properties.shadow_indexing = get_shadow_indexing_mode(config_entries);
    cfg.properties.read_replica_bucket = get_string_value(
      config_entries, topic_property_read_replica);
    cfg.properties.batch_max_bytes = get_config_value<uint32_t>(
      config_entries, topic_property_max_message_bytes);
    if (cfg.properties.read_replica_bucket.has_value()) {
        cfg.properties.read_replica = true;
    }

    cfg.properties.retention_local_target_bytes = get_tristate_value<size_t>(
      config_entries, topic_property_retention_local_target_bytes);
    cfg.properties.retention_local_target_ms
      = get_tristate_value<std::chrono::milliseconds>(
        config_entries, topic_property_retention_local_target_ms);

    cfg.properties.remote_delete
      = get_bool_value(config_entries, topic_property_remote_delete)
          .value_or(storage::ntp_config::default_remote_delete);

    cfg.properties.segment_ms = get_tristate_value<std::chrono::milliseconds>(
      config_entries, topic_property_segment_ms);

    cfg.properties.initial_retention_local_target_bytes
      = get_tristate_value<size_t>(
        config_entries, topic_property_initial_retention_local_target_bytes);
    cfg.properties.initial_retention_local_target_ms
      = get_tristate_value<std::chrono::milliseconds>(
        config_entries, topic_property_initial_retention_local_target_ms);

    cfg.properties.mpx_virtual_cluster_id
      = get_config_value<model::vcluster_id>(
        config_entries, topic_property_mpx_virtual_cluster_id);

    cfg.properties.write_caching = get_enum_value<model::write_caching_mode>(
      config_entries, topic_property_write_caching);

    cfg.properties.flush_ms = get_duration_value<std::chrono::milliseconds>(
      config_entries, topic_property_flush_ms, true);

    cfg.properties.flush_bytes = get_config_value<size_t>(
      config_entries, topic_property_flush_bytes);

    cfg.properties.tombstone_retention_ms
      = get_duration_value<std::chrono::milliseconds>(
        config_entries, topic_property_tombstone_retention_ms);

    // Attempt to set tombstone_retention_ms with the cluster default, if it
    // does not have an assigned value.
    if (!cfg.properties.tombstone_retention_ms.has_value()) {
        cfg.properties.tombstone_retention_ms
          = config::shard_local_cfg().tombstone_retention_ms();
    }

    schema_id_validation_config_parser schema_id_validation_config_parser{
      cfg.properties};

    for (auto& p : t.configs) {
        schema_id_validation_config_parser(
          p, kafka::config_resource_operation::set);
    }

    /// Final topic_property not decoded here is \ref remote_topic_properties,
    /// is more of an implementation detail no need to ever show user

    auto ret = cluster::custom_assignable_topic_configuration(cfg);
    /**
     * handle custom assignments
     */
    if (!t.assignments.empty()) {
        /**
         * custom assigned partitions must have the same replication factor
         */
        ret.cfg.partition_count = t.assignments.size();
        ret.cfg.replication_factor = t.assignments.front().broker_ids.size();
        for (auto& assignment : t.assignments) {
            ret.custom_assignments.push_back(
              cluster::custom_partition_assignment{
                .id = assignment.partition_index,
                .replicas = std::vector<model::node_id>{
                  assignment.broker_ids.begin(), assignment.broker_ids.end()}});
        }
    }
    return ret;
}

static std::vector<kafka::creatable_topic_configs>
convert_topic_configs(config_response_container_t&& topic_cfgs) {
    auto configs = std::vector<kafka::creatable_topic_configs>();
    configs.reserve(topic_cfgs.size());

    for (auto& conf : topic_cfgs) {
        configs.push_back(conf.to_create_config());
    }

    return configs;
}

std::vector<kafka::creatable_topic_configs> report_topic_configs(
  const cluster::metadata_cache& metadata_cache,
  const cluster::topic_properties& topic_properties) {
    auto topic_cfgs = make_topic_configs(
      metadata_cache, topic_properties, std::nullopt, false, false);

    return convert_topic_configs(std::move(topic_cfgs));
}

} // namespace kafka
