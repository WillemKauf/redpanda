load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "reducer_test",
    timeout = "short",
    srcs = [
        "reducer_test.cc",
    ],
    deps = [
        "//src/v/compaction",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
