/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "cluster/topic_table.h"
#include "model/fundamental.h"
#include "ssx/sformat.h"

#include <seastar/util/log.hh>

namespace detail {

template<typename T, typename... Args>
struct pack_contains : std::disjunction<std::is_same<T, Args>...> {};

template<typename T, typename... Args>
inline constexpr bool pack_contains_v = pack_contains<T, Args...>::value;

// Usage examples
static_assert(pack_contains_v<int, float, double, int>);
static_assert(!pack_contains_v<int, float, double>);

} // namespace detail

class tidp_logger {
public:
    explicit tidp_logger(
      ss::logger& logger, ss::sharded<cluster::topic_table>* topic_table)
      : _logger(logger)
      , _topic_table(topic_table) {}

    template<typename... Args>
    void log(ss::log_level lvl, const char* format, Args&&... args) const {
        if (_logger.is_enabled(lvl)) {
            if constexpr (
              detail::pack_contains_v<model::topic_id_partition, Args...>
              || detail::pack_contains_v<model::topic_id, Args...>) {
                transform_args(
                  [&](auto&&... transformed) {
                      _logger.log(
                        lvl,
                        format,
                        std::forward<decltype(transformed)>(transformed)...);
                  },
                  std::forward<Args>(args)...);
            } else {
                _logger.log(lvl, format, std::forward<Args>(args)...);
            }
        }
    }

    template<typename... Args>
    void error(const char* format, Args&&... args) const {
        log(ss::log_level::error, format, std::forward<Args>(args)...);
    }

    template<typename... Args>
    void warn(const char* format, Args&&... args) const {
        log(ss::log_level::warn, format, std::forward<Args>(args)...);
    }

    template<typename... Args>
    void info(const char* format, Args&&... args) const {
        log(ss::log_level::info, format, std::forward<Args>(args)...);
    }

    template<typename... Args>
    void debug(const char* format, Args&&... args) const {
        log(ss::log_level::debug, format, std::forward<Args>(args)...);
    }

    template<typename... Args>
    void trace(const char* format, Args&&... args) const {
        log(ss::log_level::trace, format, std::forward<Args>(args)...);
    }

    const ss::logger& logger() const { return _logger; }

private:
    friend struct tidp_logger_fixture;
    template<typename F, typename... Args>
    void transform_args(F&& f, Args&&... args) const {
        auto transform = [this](auto&& arg) -> decltype(auto) {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, model::topic_id_partition>) {
                auto topic_name = _topic_table->local().get_name_by_id(
                  arg.topic_id);
                if (topic_name.has_value()) {
                    auto ntp = model::ntp(
                      topic_name->ns, topic_name->tp, arg.partition);
                    return ssx::sformat("{}", ntp);
                } else {
                    return ssx::sformat("{}", arg);
                }
            } else if constexpr (std::is_same_v<T, model::topic_id>) {
                auto topic_name = _topic_table->local().get_name_by_id(arg);
                if (topic_name.has_value()) {
                    auto tp = model::topic_namespace_view(
                      topic_name->ns, topic_name->tp);
                    return ssx::sformat("{}", tp);
                } else {
                    return ssx::sformat("{}", arg);
                }
            } else {
                return std::forward<decltype(arg)>(arg);
            }
        };

        std::invoke(std::forward<F>(f), transform(std::forward<Args>(args))...);
    }

    ss::logger& _logger;
    ss::sharded<cluster::topic_table>* _topic_table;
};
