load('//:copts.bzl', 'PAIN_COPTS', 'PAIN_LINKOPTS', 'PAIN_TEST_COPTS')

cc_library(
    name = "pain_core",
    srcs = glob(["*.cc", "*.h"]),
    copts = PAIN_COPTS,
    linkopts = PAIN_LINKOPTS,
    deps = [
        "//src/base:pain_base",
        "//src/deva/sdk:deva_sdk",
        "//protocols/pain/proto:cc_pain_proto",
        "//protocols/pain/proto:cc_pain_errno_proto",
        "//protocols/pain/proto:cc_pain_deva_proto",
        "//protocols/pain/proto:cc_pain_asura_proto",
        "//include/pain:pain_headers",
    ],
    visibility = ["//visibility:public"],
)
