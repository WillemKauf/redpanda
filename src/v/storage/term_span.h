/*
 * Copyright 2026 Redpanda Data, Inc.
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
#include "base/vassert.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "serde/envelope.h"

#include <algorithm>
#include <optional>
#include <utility>

namespace storage {

struct term_span
  : serde::envelope<term_span, serde::version<0>, serde::compat_version<0>> {
    model::offset base;
    model::term_id term;

    auto serde_fields() { return std::tie(base, term); }

    friend bool operator==(const term_span&, const term_span&) = default;
};

/// The term spans of one segment, with the invariants enforced in one place:
/// the set always contains at least the base span (the term encoded in the
/// segment filename), and spans are strictly monotonic in both base offset
/// and term.
class term_span_set {
public:
    /// C-tor which initializes the set with the base span.
    term_span_set(model::term_id base_term, model::offset base_offset) {
        _spans.push_back(term_span{.base = base_offset, .term = base_term});
    }

    /// Adopt spans from an untrusted source (e.g. a segment index file).
    /// Returns nullopt when the spans do not satisfy the set's invariants.
    static std::optional<term_span_set> parse(chunked_vector<term_span> spans) {
        if (spans.empty()) {
            return std::nullopt;
        }
        auto non_monotonic = [](const term_span& a, const term_span& b) {
            return b.base <= a.base || b.term <= a.term;
        };
        if (std::ranges::adjacent_find(spans, non_monotonic) != spans.end()) {
            return std::nullopt;
        }
        return term_span_set(std::move(spans));
    }

    term_span_set copy() const { return term_span_set(_spans.copy()); }

    /// Adds a new span to the set.
    void add(model::term_id t, model::offset base) {
        const auto& last = _spans.back();
        vassert(
          t > last.term && base > last.base,
          "term spans must be monotonic: adding ({}, {}) after ({}, {})",
          t,
          base,
          last.term,
          last.base);
        _spans.push_back(term_span{.base = base, .term = t});
    }

    /// Performs suffix truncation, dropping spans beginning after the new last
    /// offset. The base span is always retained.
    void truncate(model::offset last_offset) {
        while (_spans.size() > 1 && _spans.back().base > last_offset) {
            _spans.pop_back();
        }
    }

    /// Rebuild from the base span plus (term, first offset) transitions in
    /// offset order, e.g. as recovered from a segment's raft configuration
    /// batches. Transitions that do not advance the term are ignored.
    void rebuild(
      const chunked_vector<std::pair<model::term_id, model::offset>>&
        transitions) {
        while (_spans.size() > 1) {
            _spans.pop_back();
        }
        for (const auto& [term, offset] : transitions) {
            if (term > last_term() && offset > last_term_base_offset()) {
                add(term, offset);
            }
        }
    }

    model::term_id base_term() const { return _spans.front().term; }
    model::term_id last_term() const { return _spans.back().term; }

    model::offset base_offset() const { return _spans.front().base; }
    model::offset last_term_base_offset() const { return _spans.back().base; }

    /// Term of the span covering offset o, treating the last span as
    /// extending indefinitely (the set does not track where the last span's
    /// covered offsets end- clamping o is the caller's business). Returns
    /// std::nullopt for offsets before the base offset.
    std::optional<model::term_id> term_at(model::offset o) const {
        if (o < base_offset()) {
            return std::nullopt;
        }
        auto it = std::ranges::upper_bound(_spans, o, {}, &term_span::base);
        return std::prev(it)->term;
    }

    /// Whether term t has a span in this set.
    bool contains(model::term_id t) const {
        return std::ranges::binary_search(_spans, t, {}, &term_span::term);
    }

    /// Base offset of the first span whose term is greater than t. For a
    /// covered term, this is the exclusive end of its coverage. Return
    /// std::nullopt when no such span exists.
    std::optional<model::offset> next_term_base(model::term_id t) const {
        auto it = std::ranges::upper_bound(_spans, t, {}, &term_span::term);
        if (it == _spans.end()) {
            return std::nullopt;
        }
        return it->base;
    }

    size_t size() const { return _spans.size(); }
    auto begin() const { return _spans.begin(); }
    auto end() const { return _spans.end(); }
    const chunked_vector<term_span>& spans() const { return _spans; }

    friend bool
    operator==(const term_span_set&, const term_span_set&) = default;

    /// (term, base offset) pairs: [(0_t, 0_o)(1_t, 10_o)(2_t, 20_o)].
    /// Reads as "offsets 0 through 9 span term 0, offsets 10 through 19 span
    /// term 1, and offsets 20 onwards span term 2".
    fmt::iterator format_to(fmt::iterator it) const {
        it = fmt::format_to(it, "[");
        for (const auto& [base, term] : _spans) {
            it = fmt::format_to(it, "({}_t, {}_o)", term, base);
        }
        return fmt::format_to(it, "]");
    }

private:
    explicit term_span_set(chunked_vector<term_span> spans)
      : _spans(std::move(spans)) {}

    chunked_vector<term_span> _spans;
};

} // namespace storage
