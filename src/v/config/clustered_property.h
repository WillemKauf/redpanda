// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "absl/container/flat_hash_map.h"
#include "config/config_not_converged.h"
#include "config/property.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>

#include <yaml-cpp/yaml.h>

#include <optional>

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
      typename property<T>::validator validator = property<T>::noop_validator,
      std::optional<legacy_default<T>> ld = std::nullopt)
      : property<T>(
          conf,
          name,
          desc,
          base_property::metadata{
            .needs_restart = needs_restart::yes,
            .visibility = meta.visibility,
            .secret = meta.secret,
            .aliases = std::move(meta.aliases)},
          std::move(def),
          std::move(validator),
          std::move(ld)) {}

    /// The active value as known by THIS node. No throw.
    const T& local_value() const { return property<T>::value(); }

    /// The cluster-converged value. Throws config_not_converged if a pending
    /// update has not yet been ratified across all alive members.
    const T& value() const {
        if (!_is_active) {
            throw config_not_converged{property<T>::name()};
        }
        return property<T>::value();
    }

    /// True iff there is no staged update pending convergence.
    bool is_active() const noexcept override { return _is_active; }

    ss::sstring staged_yaml_string() const override {
        if (!_staged.has_value()) {
            return {};
        }
        return ss::sstring{YAML::Dump(YAML::Node{*_staged})};
    }

    const std::optional<T>& staged() const noexcept { return _staged; }

    /// Resolves once is_active() becomes true, then returns value().
    /// Abort-source aware so it unblocks during shutdown.
    ss::future<const T&> wait_until_active(ss::abort_source& as) {
        if (_is_active) {
            co_return property<T>::value();
        }
        as.check();
        auto sub = as.subscribe([this]() noexcept {
            _activation_cv.broken(
              std::make_exception_ptr(ss::abort_requested_exception{}));
        });
        co_await _activation_cv.wait([this] { return _is_active; });
        co_return property<T>::value();
    }

    bool set_pending_value(YAML::Node n) override {
        auto v = n.as<T>();
        property<T>::set_pending_value(std::move(n));
        set_staged_value(std::move(v));
        return true;
    }

    /// Called by the STM apply path when a new value is staged for
    /// cluster-wide convergence.
    void set_staged_value(T v) {
        _staged = std::move(v);
        _is_active = false;
    }

    /// Called by the STM apply path when all nodes have converged.
    void complete_activation() {
        _staged.reset();
        _is_active = true;
        _activation_cv.broadcast();
    }

    void clear_staged() noexcept { _staged.reset(); }

    void apply_activation(std::string_view serialized_value) override {
        if (!_staged.has_value()) {
            return;
        }
        std::optional<T> incoming;
        try {
            incoming = YAML::Load(std::string{serialized_value}).as<T>();
        } catch (...) {
            return;
        }
        if (!incoming || *incoming != *_staged) {
            return;
        }
        complete_activation();
    }

    void test_set_staged(T v) { set_staged_value(std::move(v)); }
    void test_activate(T v) {
        if (_staged && *_staged == v) {
            complete_activation();
        }
    }
    void test_promote_local(T v) { property<T>::set_value(std::move(v)); }
    void set_value_and_activate(T v) {
        property<T>::set_value(v);
        set_staged_value(std::move(v));
        complete_activation();
    }

    bool is_clustered() const noexcept override { return true; }

    ss::sstring to_yaml_string_local() const override {
        return ss::sstring{YAML::Dump(YAML::Node{property<T>::value()})};
    }

    T operator()() const = delete;

private:
    std::optional<T> _staged;
    bool _is_active{true};
    ss::condition_variable _activation_cv;
};

class config_store;

/// Walk the config_store and emit a map of {name -> serialized local value}
/// for every clustered property. Called by the health-report sender.
absl::flat_hash_map<ss::sstring, ss::sstring>
collect_clustered_config(const config_store& store);

} // namespace config
