load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "compaction_filter_test",
    timeout = "short",
    srcs = [
        "compaction_filter_test.cc",
    ],
    deps = [
        "//src/v/compaction:key",
        "//src/v/compaction:key_offset_map",
        "//src/v/compaction:reducer",
        "//src/v/compaction:utils",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/model",
        "//src/v/model:batch_builder",
        "//src/v/model/tests:random",
        "//src/v/storage:storage",
        "//src/v/storage/compaction:compaction_filter",
        "//src/v/storage/compaction:compaction_state",
        "//src/v/storage/tests:batch_generators",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
