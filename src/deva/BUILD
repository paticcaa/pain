load('//:copts.bzl', 'PAIN_COPTS', 'PAIN_LINKOPTS', 'PAIN_TEST_COPTS')

cc_binary(
    name = "deva",
    srcs = ["main.cc"],
    copts = PAIN_COPTS,
    linkopts = PAIN_LINKOPTS,
    deps = [
        "//src/base:pain_base",
        "//src/deva:deva_core",
        "@brpc",
        "@braft",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "deva_core",
    srcs = glob(["*.cc", "*.h"], exclude = ["main.cc"]),
    copts = PAIN_COPTS,
    linkopts = PAIN_LINKOPTS,
    deps = [
        "//src/base:pain_base",
        "//protocols/pain/proto:cc_pain_deva_proto",
        "@brpc",
        "@braft",
        "@rocksdb",
        "@boost.smart_ptr",
        "@magic_enum",
    ],
)

cc_test(
    name = "test_deva",
    srcs = glob(["test/*.cc"]),
    copts = PAIN_TEST_COPTS,
    linkopts = PAIN_LINKOPTS,
    linkstatic = True,
    deps = [
        "//src/deva:deva_core",
        "@googletest//:gtest",
        "@googletest//:gtest_main",
    ],
)
