load("//bazel:build.bzl", "redpanda_cc_library")

package(
    default_visibility = ["//src/v/dedup:__subpackages__"],
)

redpanda_cc_library(
    name = "windowed_dedup_map",
    srcs = [
        "windowed_dedup_map.cc",
    ],
    hdrs = [
        "windowed_dedup_map.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/hashing:xx",
        "//src/v/model",
        "//src/v/serde/parquet:bloom_filter",
        "//src/v/ssx:future_util",
        "//src/v/utils:file_io",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "produce_filter",
    srcs = [
        "produce_filter.cc",
    ],
    hdrs = [
        "produce_filter.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":windowed_dedup_map",
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "@seastar",
    ],
)
