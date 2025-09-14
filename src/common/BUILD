load('//:copts.bzl', 'PAIN_COPTS', 'PAIN_LINKOPTS', 'PAIN_TEST_COPTS')

cc_library(
    name = "pain_common",
    srcs = glob(["*.h", "*.cc"]),
    copts = PAIN_COPTS,
    linkopts = PAIN_LINKOPTS,
    deps = [
        "//src/base:pain_base",
        "//protocols/pain/proto:cc_pain_common_proto",
        "@boost.assert",
        "@boost.smart_ptr",
        "@rocksdb",
        "@braft",
    ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "test_pain_common",
    srcs = glob(["test/*.cc"]),
    copts = PAIN_TEST_COPTS,
    linkopts = PAIN_LINKOPTS,
    linkstatic = True,
    deps = [
        "//src/common:pain_common",
        "@googletest//:gtest",
        "@googletest//:gtest_main",
    ],
)
