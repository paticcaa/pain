load('//:copts.bzl', 'PAIN_COPTS', 'PAIN_LINKOPTS', 'PAIN_TEST_COPTS')

cc_binary(
    name = "manusya",
    srcs = ["main.cc"],
    copts = PAIN_COPTS,
    linkopts = PAIN_LINKOPTS,
    deps = [
        "//src/base:pain_base",
        "//src/manusya:manusya_core",
        "@brpc",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "manusya_core",
    srcs = glob(["*.cc", "*.h"], exclude = ["main.cc"]),
    copts = PAIN_COPTS,
    linkopts = PAIN_LINKOPTS,
    deps = [
        "//src/base:pain_base",
        "//protocols/pain/proto:cc_pain_manusya_proto",
        "@brpc",
        "@boost.smart_ptr",
        "@boost.intrusive",
    ],
)


cc_test(
    name = "test_manusya",
    srcs = glob(["test/*.cc"]),
    copts = PAIN_TEST_COPTS,
    linkopts = PAIN_LINKOPTS,
    linkstatic = True,
    deps = [
        ":manusya_core",
        "@googletest//:gtest",
        "@googletest//:gtest_main",
    ],
)
