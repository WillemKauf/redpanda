/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/level_one/compaction/meta.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cluster/metadata_cache.h"
#include "cluster/types.h"

namespace cloud_topics::l1 {

// A wrapper around a `metadata_cache` to provide easy mocking and break
// dependency on a multitude of cluster objects within the `log_sampler`.
class topic_cfg_provider {
public:
    virtual ~topic_cfg_provider() noexcept = default;

    virtual std::optional<
      std::reference_wrapper<const cluster::topic_configuration>>
      get_topic_cfg(model::topic_namespace_view) const = 0;
};

class topic_cfg_provider_impl : public topic_cfg_provider {
public:
    topic_cfg_provider_impl(cluster::metadata_cache*);

    std::optional<std::reference_wrapper<const cluster::topic_configuration>>
      get_topic_cfg(model::topic_namespace_view) const final;

private:
    cluster::metadata_cache* _metadata_cache;
};

// Responsible for issuing `get_compaction_info()` requests to the `metastore`
// when attempting to schedule a round of compactions. This class does not make
// any decisions about whether a `log` needs compacting or not, nor does it
// filter out sampled logs that may not need compaction in its returned
// container from `sample_logs()`. Scheduling decisions such as those are left
// to the other components in the `cloud_topics` compaction subsystem.
class log_sampler {
public:
    log_sampler(metastore*, std::unique_ptr<topic_cfg_provider>);

    // Populates a vector of `log_info_and_meta` from the provided `log_list_t`
    // by sampling each log's compaction info from the metastore. It is not
    // guaranteed that every log present in `log_list_t` will have an entry in
    // the returned vector, e.g. due to concurrent removal or metastore errors.
    ss::future<>
    sample_logs(logs_type_t&, log_list_t&, log_compaction_queue&) const;

private:
    // Owned by `app`.
    metastore* _metastore;

    std::unique_ptr<topic_cfg_provider> _topic_metadata_provider;
};

log_sampler make_default_log_sampler(metastore*, cluster::metadata_cache*);

} // namespace cloud_topics::l1
