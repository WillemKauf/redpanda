/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "storage/kvstore.h"
#include "utils/fragmented_vector.h"

#include <seastar/core/gate.hh>
#include <seastar/core/timer.hh>

namespace kafka {

/// Main structure of statistics that are being accounted for. These are
/// periodically serialized to disk, hence why the struct inherits from the
/// serde::envelope
struct usage
  : serde::envelope<usage, serde::version<0>, serde::compat_version<0>> {
    uint64_t bytes_sent{0};
    uint64_t bytes_received{0};
    std::optional<uint64_t> bytes_cloud_storage;
    usage operator+(const usage&) const;
    auto serde_fields() {
        return std::tie(bytes_sent, bytes_received, bytes_cloud_storage);
    }
};

struct usage_window
  : serde::envelope<usage_window, serde::version<0>, serde::compat_version<0>> {
    uint64_t begin{0};
    uint64_t end{0};
    usage u;

    /// Only valid to assert these conditions internal to usage manager. When it
    /// returns windows as results these conditions may not hold.
    bool is_uninitialized() const { return begin == 0 && end == 0; }
    bool is_open() const { return begin != 0 && end == 0; }

    void reset(ss::lowres_system_clock::time_point now);

    auto serde_fields() { return std::tie(begin, end, u); }
};

template<typename clock_type = ss::lowres_clock>
class usage_aggregator {
public:
    usage_aggregator(
      storage::kvstore& kvstore,
      size_t usage_num_windows,
      std::chrono::seconds usage_window_width_interval,
      std::chrono::seconds usage_disk_persistance_interval);
    virtual ~usage_aggregator() = default;

    virtual ss::future<> start();
    virtual ss::future<> stop();

    std::vector<usage_window> get_usage_stats();

    std::chrono::seconds max_history() const {
        return _usage_window_width_interval * _usage_num_windows;
    }

protected:
    /// Portions of the implementation that rely on higher level constructs
    /// to obtain the data are not relevent to unit testing this class and
    /// can be broken out so this class can be made more testable
    virtual ss::future<usage> close_current_window() = 0;

private:
    std::chrono::seconds reset_state(fragmented_vector<usage_window> buckets);
    void close_window();
    void rearm_window_timer();
    bool is_bucket_stale(size_t idx, uint64_t close_ts) const;

private:
    size_t _usage_num_windows;
    std::chrono::seconds _usage_window_width_interval;
    std::chrono::seconds _usage_disk_persistance_interval;

    /// Timers for controlling window closure and disk persistance
    ss::timer<clock_type> _timer;
    ss::timer<clock_type> _persist_disk_timer;

    ss::gate _bg_write_gate;
    ss::gate _gate;
    size_t _current_window{0};
    fragmented_vector<usage_window> _buckets;
    storage::kvstore& _kvstore;
};
} // namespace kafka
