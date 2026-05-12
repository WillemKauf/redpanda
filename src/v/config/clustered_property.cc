// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/clustered_property.h"

#include "config/config_store.h"

namespace config {

absl::flat_hash_map<ss::sstring, ss::sstring>
collect_clustered_config(const config_store& store) {
    absl::flat_hash_map<ss::sstring, ss::sstring> out;
    store.for_each([&](const base_property& p) {
        if (p.is_clustered()) {
            out.emplace(ss::sstring{p.name()}, p.to_yaml_string_local());
        }
    });
    return out;
}

} // namespace config
