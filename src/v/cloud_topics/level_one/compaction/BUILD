load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/cloud_topics/level_one:__subpackages__"])

redpanda_cc_library(
    name = "logger",
    hdrs = [
        "logger.h",
    ],
    visibility = [
        "//src/v/cloud_topics/level_one:__subpackages__",
    ],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "source",
    srcs = [
        "source.cc",
    ],
    hdrs = [
        "source.h",
    ],
    visibility = [
        "//src/v/cloud_topics/level_one:__subpackages__",
    ],
    deps = [
        "//src/v/compaction:reducer",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "sink",
    srcs = [
        "sink.cc",
    ],
    hdrs = [
        "sink.h",
    ],
    visibility = [
        "//src/v/cloud_topics/level_one:__subpackages__",
    ],
    deps = [
        "//src/v/bytes",
        "//src/v/bytes:iostream",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/compaction:reducer",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/model:batch_compression",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "filter",
    srcs = [
        "filter.cc",
    ],
    hdrs = [
        "filter.h",
    ],
    visibility = [
        "//src/v/cloud_topics/level_one:__subpackages__",
    ],
    deps = [
        "//src/v/compaction:filter",
        "//src/v/compaction:key_offset_map",
        "//src/v/compaction:utils",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "worker",
    srcs = [
        "worker.cc",
    ],
    hdrs = [
        "worker.h",
    ],
    visibility = [
        "//src/v/cloud_topics/level_one:__subpackages__",
    ],
    deps = [
        ":logger",
        ":sink",
        ":source",
        "//src/v/compaction:reducer",
        "//src/v/model",
        "//src/v/ssx:future_util",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "executor",
    srcs = [
        "executor.cc",
    ],
    hdrs = [
        "executor.h",
    ],
    visibility = [
        "//src/v/cloud_topics/level_one:__subpackages__",
    ],
    deps = [
        ":logger",
        ":meta",
        ":worker",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "//src/v/ssx:future_util",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "scheduling_policies",
    srcs = [
        "scheduling_policies.cc",
    ],
    hdrs = [
        "scheduling_policies.h",
    ],
    implementation_deps = [
        ":executor",
        "//src/v/random:generators",
    ],
    visibility = [
        "//src/v/cloud_topics/level_one:__subpackages__",
    ],
    deps = [
        ":meta",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "meta",
    hdrs = [
        "meta.h",
    ],
    visibility = [
        "//src/v/cloud_topics/level_one:__subpackages__",
    ],
    deps = [
        "//src/v/container:intrusive",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "scheduler",
    srcs = [
        "scheduler.cc",
        "log_collector_impl.cc",
    ],
    hdrs = [
        "scheduler.h",
        "log_collector_impl.h",
    ],
    implementation_deps = [
        "//src/v/ssx:future_util",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":executor",
        ":log_collector",
        ":logger",
        ":meta",
        ":scheduling_policies",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:intrusive",
        "//src/v/model",
        "//src/v/ssx:semaphore",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "log_collector",
    hdrs = [
        "log_collector.h",
    ],
    implementation_deps = [
        "//src/v/ssx:future_util",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@seastar",
    ],
)
