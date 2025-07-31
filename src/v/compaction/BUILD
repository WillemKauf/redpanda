load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "compaction",
    hdrs = [
        "reducer.h",
    ],
    include_prefix = "compaction",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/model",
        "@seastar",
    ],
)
