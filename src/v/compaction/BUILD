load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "compaction",
    srcs = [
        "filter.cc",
        "reducer.cc",
        "types.cc",
    ],
    hdrs = [
        "filter.h",
        "fwd.h",
        "reducer.h",
        "types.h",
    ],
    implementation_deps = [
        ":logger",
        "//src/v/utils:to_string",
        "@fmt",
    ],
    include_prefix = "compaction",
    visibility = ["//visibility:public"],
    deps = [
        ":key_offset_map",
        ":types",
        ":utils",
        "//src/v/base",
        "//src/v/model",
        "//src/v/storage:file_sanitizer_types",
        "//src/v/storage:parser_utils",
        "//src/v/storage:scoped_file_tracker",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "types",
    srcs = [
        "types.cc",
    ],
    hdrs = [
        "types.h",
    ],
    implementation_deps = [
        "//src/v/utils:to_string",
        "@fmt",
    ],
    include_prefix = "compaction",
    visibility = ["//visibility:public"],
    deps = [
        ":key_offset_map",
        "//src/v/base",
        "//src/v/model",
        "//src/v/storage:file_sanitizer_types",
        "//src/v/storage:scoped_file_tracker",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "key",
    srcs = [
        "key.cc",
    ],
    hdrs = [
        "key.h",
    ],
    include_prefix = "compaction",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes",
        "//src/v/model",
    ],
)

redpanda_cc_library(
    name = "key_offset_map",
    srcs = [
        "key_offset_map.cc",
    ],
    hdrs = [
        "key_offset_map.h",
    ],
    include_prefix = "compaction",
    visibility = ["//visibility:public"],
    deps = [
        ":key",
        "//src/v/container:fragmented_vector",
        "//src/v/hashing:secure",
        "//src/v/model",
        "//src/v/utils:tracking_allocator",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "utils",
    srcs = [
        "utils.cc",
    ],
    hdrs = [
        "utils.h",
    ],
    include_prefix = "compaction",
    visibility = ["//visibility:public"],
    deps = [
        ":key_offset_map",
        "//src/v/model",
        "//src/v/storage:parser_utils",
    ],
)

redpanda_cc_library(
    name = "logger",
    srcs = [
        "logger.cc",
    ],
    hdrs = [
        "logger.h",
    ],
    include_prefix = "compaction",
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)
