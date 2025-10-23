// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tests/topic_table_fixture.h"
#include "cluster/topic_table.h"
#include "utils/tidp_logger.h"

#include <gtest/gtest.h>

static ss::logger test_log("tidp_logger_test");

struct tidp_logger_fixture
  : public topic_table_fixture
  , public ::testing::Test {
    template<typename F, typename... Args>
    void transform_args(tidp_logger& logger, F&& f, Args&&... args) const {
        logger.transform_args(std::forward<F>(f), std::forward<Args>(args)...);
    }
};

TEST_F(tidp_logger_fixture, tidp_logger_test) {
    tidp_logger tp_logger(test_log, &table);
    create_topics();
    for (const auto& [tp, md] : table.local().topics_map()) {
        auto& tp_id = md.get_configuration().tp_id;
        auto check_transformation_func = [tp](ss::sstring transformed) {
            ASSERT_EQ(ssx::sformat("{}", tp), transformed);
        };
        transform_args(tp_logger, check_transformation_func, tp_id.value());
    }
    vlog(tp_logger.info, "this is whatever {}", 12345);
    ASSERT_TRUE(false);
}
