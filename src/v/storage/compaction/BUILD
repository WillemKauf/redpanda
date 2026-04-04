load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "compaction_state",
    hdrs = [
        "compaction_state.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/compaction:state",
        "//src/v/model",
    ],
)

redpanda_cc_library(
    name = "compaction_filter",
    srcs = [
        "compaction_filter.cc",
    ],
    hdrs = [
        "compaction_filter.h",
    ],
    implementation_deps = [
        "//src/v/compaction:utils",
        "//src/v/model",
        "//src/v/storage",
        "//src/v/storage:logger",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":compaction_state",
        "//src/v/compaction:filter",
        "//src/v/compaction:key_offset_map",
        "@seastar",
    ],
)
