load('//:copts.bzl', 'PAIN_COPTS', 'PAIN_LINKOPTS', 'PAIN_TEST_COPTS')

cc_library(
    name = "pain_base",
    srcs = glob(["*.h", "*.cc"]),
    copts = PAIN_COPTS,
    linkopts = PAIN_LINKOPTS,
    deps = [
        "@opentelemetry-cpp//api",
        "@opentelemetry-cpp//exporters/otlp:otlp_http_exporter",
        "@opentelemetry-cpp//exporters/ostream:ostream_span_exporter",
        "@opentelemetry-cpp//sdk:headers",
        "@opentelemetry-cpp//sdk/src/trace",
        "@spdlog",
        "@boost.assert",
        "@boost.stacktrace",
        "@boost.preprocessor",
        "@uuid_v4//:uuid_v4",
        "@brpc",
    ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "test_pain_base_result",
    srcs = glob(["test/test_result.cc"]),
    copts = PAIN_TEST_COPTS,
    linkopts = PAIN_LINKOPTS,
    linkstatic = True,
    deps = [
        "//src/base:pain_base",
        "@googletest//:gtest",
        "@googletest//:gtest_main",
    ],
)

cc_test(
    name = "test_pain_base_rusty_result",
    srcs = glob(["test/test_rusty_result.cc"]),
    copts = PAIN_COPTS,
    linkopts = PAIN_LINKOPTS,
    linkstatic = True,
    deps = [
        "//src/base:pain_base",
        "@googletest//:gtest",
        "@googletest//:gtest_main",
    ],
)
