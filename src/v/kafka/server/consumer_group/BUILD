load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "types",
    srcs = [
        "types.cc",
    ],
    hdrs = [
        "types.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/kafka/protocol",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/serde:chrono",
        "//src/v/serde:enum",
        "//src/v/serde:optional",
        "//src/v/serde:vector",
        "//src/v/utils:named_type",
        "@abseil-cpp//absl/container:node_hash_map",
    ],
)

redpanda_cc_library(
    name = "commands",
    hdrs = [
        "commands.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":types",
        "//src/v/serde",
        "//src/v/utils:named_type",
    ],
)

redpanda_cc_library(
    name = "consumer_group_stm",
    srcs = [
        "consumer_group_stm.cc",
    ],
    hdrs = [
        "consumer_group_stm.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":commands",
        ":types",
        "//src/v/cluster:state_machine_registry",
        "//src/v/container:chunked_vector",
        "//src/v/kafka/server:logger",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/serde",
        "//src/v/storage",
        "@abseil-cpp//absl/container:node_hash_map",
    ],
)
