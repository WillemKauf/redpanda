/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "base/format_to.h"
#include "container/chunked_vector.h"
#include "container/interval_set.h"
#include "serde/envelope.h"
#include "serde/rw/envelope.h"

namespace container {

/// \brief Wrapper around interval_set with an interface suited for inclusive
/// offset ranges. Template parameter OffsetT must be a named_type wrapping an
/// integral (e.g. kafka::offset, model::offset).
template<typename OffsetT>
class offset_interval_set
  : public serde::envelope<
      offset_interval_set<OffsetT>,
      serde::version<0>,
      serde::compat_version<0>> {
public:
    using iset_t = interval_set<typename OffsetT::type>;
    bool operator==(const offset_interval_set&) const = default;
    auto serde_fields() { return std::tie(iset_); }

    struct interval {
        OffsetT base_offset;
        OffsetT last_offset;

        fmt::iterator format_to(fmt::iterator it) const {
            return fmt::format_to(it, "[{}, {}]", base_offset, last_offset);
        }
    };

    template<bool reverse = false>
    class stream {
    public:
        using iterator_t = std::conditional_t<
          reverse,
          typename iset_t::const_reverse_iterator,
          typename iset_t::const_iterator>;

        explicit stream(const iset_t& underlying)
          : set_(underlying) {
            if constexpr (reverse) {
                iter_ = set_.rbegin();
                end_ = set_.rend();
            } else {
                iter_ = set_.begin();
                end_ = set_.end();
            }
        }
        bool has_next() const noexcept { return iter_ != end_; }
        interval next() {
            vassert(has_next(), "next() called while has_next() is false");
            interval ret{
              .base_offset = OffsetT(iter_->first),
              .last_offset = OffsetT(iter_->second - 1),
            };
            ++iter_;
            return ret;
        }

    private:
        const iset_t& set_;
        iterator_t iter_;
        iterator_t end_;
    };

    bool empty() const { return iset_.empty(); }

    bool insert(OffsetT base, OffsetT last) {
        auto len = last() - base() + 1;
        return iset_.insert(typename iset_t::interval{base(), len}).second;
    }

    bool contains(OffsetT offset) const {
        return iset_.find(offset()) != iset_.end();
    }

    bool covers(OffsetT start, OffsetT end) const {
        auto it = iset_.find(start());
        if (it == iset_.end()) {
            return false;
        }
        return (it->first <= start() && it->second > end());
    }

    stream<false> make_stream() const { return stream<false>(iset_); }
    stream<true> make_reverse_stream() const { return stream<true>(iset_); }

    chunked_vector<interval> to_vec() const {
        chunked_vector<interval> ret;
        ret.reserve(iset_.size());
        auto s = make_stream();
        while (s.has_next()) {
            ret.emplace_back(s.next());
        }
        return ret;
    }

    void truncate_with_new_start_offset(OffsetT new_start_offset) {
        // First, remove all intervals that are fully below the new start.
        while (!iset_.empty()) {
            auto begin_it = iset_.begin();
            auto begin_last_offset = OffsetT{iset_.to_end(begin_it) - 1};
            if (begin_last_offset >= new_start_offset) {
                // This interval is partially or entirely above the new start.
                // Handle below.
                break;
            }
            // This interval is entirely below the new start.
            iset_.erase(begin_it);
        }
        if (iset_.empty()) {
            return;
        }
        auto begin_it = iset_.begin();
        auto begin_base_offset = OffsetT{iset_.to_start(begin_it)};
        if (begin_base_offset >= new_start_offset) {
            // This interval starts above or is aligned exactly with the new
            // start.
            return;
        }
        // This interval is partially below the new start. Replace it with an
        // interval that is aligned with the new start.
        auto begin_last_offset = OffsetT{iset_.to_end(begin_it) - 1};
        iset_.erase(begin_it);
        insert(new_start_offset, begin_last_offset);
    }

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{}", iset_);
    }

private:
    iset_t iset_;
};

} // namespace container
