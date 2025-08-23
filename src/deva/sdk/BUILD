load('//:copts.bzl', 'PAIN_COPTS', 'PAIN_LINKOPTS', 'PAIN_TEST_COPTS')

cc_library(
    name = "deva_sdk",
    srcs = ["rpc_client.cc", "rpc_client.h"],
    copts = PAIN_COPTS,
    linkopts = PAIN_LINKOPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//src/base:pain_base",
        "//protocols/pain/proto:cc_pain_deva_proto",
        "@brpc",
        "@braft",
    ],
)
