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
#include "model/fundamental.h"
#include "storage/term_span.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

namespace storage {

namespace {
chunked_vector<term_span>
make_spans(std::initializer_list<std::pair<int64_t, int64_t>> spans) {
    chunked_vector<term_span> v;
    for (const auto& [term, base] : spans) {
        v.push_back(
          term_span{.base = model::offset(base), .term = model::term_id(term)});
    }
    return v;
}
} // namespace

TEST(term_span_set, base_span) {
    term_span_set set(model::term_id{3}, model::offset{10});

    EXPECT_EQ(set.size(), 1);
    EXPECT_EQ(set.base_term(), model::term_id{3});
    EXPECT_EQ(set.base_offset(), model::offset{10});
    EXPECT_EQ(set.last_term(), model::term_id{3});
    EXPECT_EQ(set.last_term_base_offset(), model::offset{10});
}

TEST(term_span_set, add_and_query) {
    term_span_set set(model::term_id{0}, model::offset{10});

    // spans: [10, 20) -> 0, [20, 50) -> 2, [50, ...) -> 5
    set.add(model::term_id{2}, model::offset{20});
    set.add(model::term_id{5}, model::offset{50});

    EXPECT_EQ(set.size(), 3);
    EXPECT_EQ(set.base_term(), model::term_id{0});
    EXPECT_EQ(set.last_term(), model::term_id{5});
    EXPECT_EQ(set.last_term_base_offset(), model::offset{50});

    EXPECT_EQ(set.term_at(model::offset{10}), model::term_id{0});
    EXPECT_EQ(set.term_at(model::offset{19}), model::term_id{0});
    EXPECT_EQ(set.term_at(model::offset{20}), model::term_id{2});
    EXPECT_EQ(set.term_at(model::offset{49}), model::term_id{2});
    EXPECT_EQ(set.term_at(model::offset{50}), model::term_id{5});
    // the last span is open-ended; bounding it is the caller's business
    EXPECT_EQ(set.term_at(model::offset{1000}), model::term_id{5});
    // offsets before the base offset have no term
    EXPECT_EQ(set.term_at(model::offset{9}), std::nullopt);

    EXPECT_TRUE(set.contains(model::term_id{0}));
    EXPECT_TRUE(set.contains(model::term_id{2}));
    EXPECT_TRUE(set.contains(model::term_id{5}));
    // terms may be skipped (failed elections)
    EXPECT_FALSE(set.contains(model::term_id{1}));
    EXPECT_FALSE(set.contains(model::term_id{6}));

    EXPECT_EQ(set.next_term_base(model::term_id{0}), model::offset{20});
    EXPECT_EQ(set.next_term_base(model::term_id{2}), model::offset{50});
    // uncovered terms answer with the first span of a greater term
    EXPECT_EQ(set.next_term_base(model::term_id{1}), model::offset{20});
    // no span's term exceeds the last one; its end is the segment's end
    EXPECT_EQ(set.next_term_base(model::term_id{5}), std::nullopt);
    EXPECT_EQ(set.next_term_base(model::term_id{6}), std::nullopt);
}

TEST(term_span_set, truncate) {
    term_span_set set(model::term_id{0}, model::offset{10});
    set.add(model::term_id{2}, model::offset{20});
    set.add(model::term_id{5}, model::offset{50});

    // truncating into the middle span drops the later span
    set.truncate(model::offset{30});
    EXPECT_EQ(set.last_term(), model::term_id{2});
    EXPECT_EQ(set.last_term_base_offset(), model::offset{20});

    // spans can be re-added after truncation
    set.add(model::term_id{3}, model::offset{31});
    EXPECT_EQ(set.last_term(), model::term_id{3});

    // truncating below the base offset retains the base span
    set.truncate(model::offset{0});
    EXPECT_EQ(set.size(), 1);
    EXPECT_EQ(set.base_term(), model::term_id{0});
    EXPECT_EQ(set.last_term(), model::term_id{0});
}

TEST(term_span_set, truncate_boundaries) {
    term_span_set set(model::term_id{0}, model::offset{10});
    set.add(model::term_id{2}, model::offset{20});
    set.add(model::term_id{5}, model::offset{50});

    // truncating to the last offset before a span's base drops the span
    set.truncate(model::offset{49});
    EXPECT_EQ(set.last_term(), model::term_id{2});

    // truncating to exactly a span's base offset keeps the span
    set.add(model::term_id{5}, model::offset{50});
    set.truncate(model::offset{50});
    EXPECT_EQ(set.last_term(), model::term_id{5});

    // same for a middle span's base offset: only later spans are dropped
    set.truncate(model::offset{20});
    EXPECT_EQ(set.last_term(), model::term_id{2});
    EXPECT_EQ(set.last_term_base_offset(), model::offset{20});
}

TEST(term_span_set, rebuild) {
    term_span_set set(model::term_id{1}, model::offset{10});
    set.add(model::term_id{2}, model::offset{20});

    // rebuilding replaces everything but the base span. Transitions that do
    // not advance the term (e.g. mid-term configuration batches) are ignored,
    // as is a transition at the base offset itself (the base term's own
    // configuration batch).
    chunked_vector<std::pair<model::term_id, model::offset>> transitions;
    transitions.emplace_back(model::term_id{1}, model::offset{10});
    transitions.emplace_back(model::term_id{1}, model::offset{15});
    transitions.emplace_back(model::term_id{3}, model::offset{30});
    transitions.emplace_back(model::term_id{3}, model::offset{35});
    transitions.emplace_back(model::term_id{6}, model::offset{60});
    set.rebuild(transitions);

    EXPECT_EQ(
      set, *term_span_set::parse(make_spans({{1, 10}, {3, 30}, {6, 60}})));
}

TEST(term_span_set, rebuild_empty_transitions) {
    term_span_set set(model::term_id{1}, model::offset{10});
    set.add(model::term_id{2}, model::offset{20});

    set.rebuild({});
    EXPECT_EQ(set.size(), 1);
    EXPECT_EQ(set.base_term(), model::term_id{1});
    EXPECT_EQ(set.base_offset(), model::offset{10});
}

TEST(term_span_set, parse) {
    auto set = term_span_set::parse(make_spans({{0, 10}, {2, 20}, {5, 50}}));
    ASSERT_TRUE(set.has_value());
    EXPECT_EQ(set->size(), 3);
    EXPECT_EQ(set->base_term(), model::term_id{0});
    EXPECT_EQ(set->last_term(), model::term_id{5});

    EXPECT_TRUE(term_span_set::parse(make_spans({{3, 0}})).has_value());

    // invalid inputs: empty, non-monotonic in either field
    EXPECT_EQ(term_span_set::parse({}), std::nullopt);
    EXPECT_EQ(
      term_span_set::parse(make_spans({{0, 10}, {2, 10}})), std::nullopt);
    EXPECT_EQ(
      term_span_set::parse(make_spans({{0, 10}, {2, 9}})), std::nullopt);
    EXPECT_EQ(
      term_span_set::parse(make_spans({{2, 10}, {2, 20}})), std::nullopt);
    EXPECT_EQ(
      term_span_set::parse(make_spans({{2, 10}, {1, 20}})), std::nullopt);
}

} // namespace storage
