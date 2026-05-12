// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include <fmt/format.h>

#include <stdexcept>
#include <string>
#include <string_view>

namespace config {

/// Thrown by clustered_property<T>::value() when a pending update has not
/// yet been ratified across all alive members of the cluster.
class config_not_converged : public std::runtime_error {
public:
    explicit config_not_converged(std::string_view property_name)
      : std::runtime_error(
          fmt::format(
            "clustered config property '{}' is not yet converged across "
            "cluster",
            property_name))
      , _property_name(property_name) {}

    std::string_view property_name() const noexcept { return _property_name; }

private:
    std::string _property_name;
};

} // namespace config
