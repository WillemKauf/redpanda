load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "windowed_dedup_map_test",
    timeout = "short",
    srcs = [
        "windowed_dedup_map_test.cc",
    ],
    deps = [
        "//src/v/bytes",
        "//src/v/dedup:windowed_dedup_map",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "produce_filter_test",
    timeout = "short",
    srcs = [
        "produce_filter_test.cc",
    ],
    deps = [
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/container:chunked_vector",
        "//src/v/dedup:produce_filter",
        "//src/v/dedup:windowed_dedup_map",
        "//src/v/model",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
