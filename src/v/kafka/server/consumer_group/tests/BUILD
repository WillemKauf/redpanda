load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "assignor_test",
    timeout = "short",
    srcs = [
        "assignor_test.cc",
    ],
    deps = [
        "//src/v/container:chunked_vector",
        "//src/v/kafka/server/consumer_group:assignor",
        "//src/v/kafka/server/consumer_group:types",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/container:node_hash_map",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "types_serde_test",
    timeout = "short",
    srcs = [
        "types_serde_test.cc",
    ],
    deps = [
        "//src/v/kafka/server/consumer_group:commands",
        "//src/v/kafka/server/consumer_group:types",
        "//src/v/serde",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "consumer_group_stm_test",
    timeout = "short",
    srcs = [
        "consumer_group_stm_test.cc",
    ],
    cpu = 3,
    deps = [
        "//src/v/kafka/server/consumer_group:commands",
        "//src/v/kafka/server/consumer_group:consumer_group_stm",
        "//src/v/kafka/server/consumer_group:types",
        "//src/v/model",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/raft/tests:raft_fixture_retry_policy",
        "//src/v/serde",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
