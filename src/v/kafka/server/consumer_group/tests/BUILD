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
