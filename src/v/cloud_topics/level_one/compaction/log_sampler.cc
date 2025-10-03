/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/log_sampler.h"

#include "cloud_topics/level_one/compaction/logger.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "compaction/utils.h"
#include "config/configuration.h"
#include "container/chunked_circular_buffer.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"

#include <seastar/core/coroutine.hh>

#include <chrono>

using namespace std::chrono_literals;

namespace cloud_topics::l1 {

log_sampler::log_sampler(
  metastore* metastore, cluster::metadata_cache* metadata_cache)
  : _metastore(metastore)
  , _metadata_cache(metadata_cache) {}

ss::future<> log_sampler::sample_logs(
  logs_type_t& logs,
  log_list_t& logs_list,
  log_compaction_queue& cached_metadata) const {
    chunked_vector<metastore::compaction_sample_spec> to_sample;

    to_sample.reserve(logs.size());

    auto now = model::timestamp::now();
    for (const auto& log : logs_list) {
        if (!log.link.is_linked()) {
            continue;
        }

        if (log.inflight) {
            // No need to sample inflight logs
            vlog(
              compaction_log.debug,
              "Skipping sample collection for CTP {}, compaction is inflight",
              log.tidp);
            continue;
        }

        if (log.info_and_ts.has_value()) {
            // TODO: maybe configure this some other way.
            auto sample_interval
              = config::shard_local_cfg().log_compaction_interval_ms();
            auto delta = to_time_point(now)
                         - to_time_point(log.info_and_ts->sampled_at);
            if (delta <= sample_interval) {
                vlog(
                  compaction_log.debug,
                  "Skipping sample collection for CTP {}, delta is less than "
                  "sample interval.",
                  log.tidp);

                continue;
            }
        }

        auto topic_cfg_opt = _metadata_cache->get_topic_metadata_ref(
          model::topic_namespace_view(log.ntp));

        if (!topic_cfg_opt.has_value()) {
            continue;
        }

        const auto& topic_cfg = topic_cfg_opt.value().get().get_configuration();
        auto tombstone_removal_ts = [&topic_cfg, now]() -> model::timestamp {
            // Cleaned ranges with tombstones that were cleaned at or below
            // tombstone_removal_upper_bound_ts are eligible to have tombstones
            // entirely removed.
            auto delete_retention_ms
              = config::shard_local_cfg().tombstone_retention_ms();
            if (topic_cfg.properties.delete_retention_ms.has_optional_value()) {
                delete_retention_ms
                  = topic_cfg.properties.delete_retention_ms.value();
            }

            if (topic_cfg.properties.delete_retention_ms.is_disabled()) {
                delete_retention_ms = std::nullopt;
            }

            return delete_retention_ms.has_value()
                     ? now - model::timestamp(delete_retention_ms->count())
                     : model::timestamp::max();
        }();
        vlog(compaction_log.debug, "Sampling CTP {}", log.tidp);

        to_sample.emplace_back(log.tidp, tombstone_removal_ts);
    }

    auto needs_compaction =
      [](const log_compaction_meta& log, const auto& topic_cfg_opt) {
          if (!topic_cfg_opt) {
              return false;
          }
          auto& topic_cfg = topic_cfg_opt.value().get().get_configuration();
          auto& topic_mcdr = topic_cfg.properties.min_cleanable_dirty_ratio;
          auto min_cleanable_dirty_ratio = topic_mcdr.has_optional_value()
                                             ? topic_mcdr.value()
                                             : config::shard_local_cfg()
                                                 .min_cleanable_dirty_ratio()
                                                 .value_or(0.0);
          auto& topic_mcl = topic_cfg.properties.max_compaction_lag_ms;
          auto max_compaction_lag_ms
            = topic_mcl.has_value()
                ? topic_mcl.value()
                : config::shard_local_cfg().max_compaction_lag_ms();
          return compaction::log_needs_compaction(
            log.info_and_ts->info.dirty_ratio,
            min_cleanable_dirty_ratio,
            log.info_and_ts->info.earliest_dirty_ts,
            max_compaction_lag_ms);
      };

    to_sample.shrink_to_fit();
    auto samples = co_await _metastore->get_compaction_infos(to_sample);

    for (auto& log : logs_list) {
        if (!log.link.is_linked()) {
            continue;
        }

        if (!samples.contains(log.tidp)) {
            // Likely this log was not sampled because the log was sampled less
            // than `sample_interval` time ago.
            continue;
        }

        auto& sample = samples.at(log.tidp);

        if (!sample.has_value()) {
            vlog(
              compaction_log.warn,
              "Failed to collect sample for CTP {} during compaction: {}",
              log.tidp,
              sample.error());
            continue;
        }

        log.info_and_ts = compaction_info_and_timestamp{
          .info = std::move(sample).value(), .sampled_at = now};

        vlog(
          compaction_log.debug,
          "Sample for CTP {} returned {}",
          log.tidp,
          log.info_and_ts->info.dirty_ratio);

        auto topic_cfg_opt = _metadata_cache->get_topic_metadata_ref(
          model::topic_namespace_view(log.ntp));
        if (needs_compaction(log, topic_cfg_opt)) {
            auto ptr_it = logs.find(log.tidp);
            if (ptr_it != logs.end()) {
                cached_metadata.push(*ptr_it);
            }
        }
    }
}

} // namespace cloud_topics::l1
