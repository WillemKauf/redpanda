load("//bazel:test.bzl", "redpanda_cc_gtest")

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
