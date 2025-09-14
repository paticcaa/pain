load('//:copts.bzl', 'PAIN_COPTS', 'PAIN_LINKOPTS', 'PAIN_TEST_COPTS')

cc_binary(
    name = "sad",
    srcs = glob(["*.cc", "*.h"]),
    copts = PAIN_COPTS,
    linkopts = PAIN_LINKOPTS,
    deps = [
        "//src/base:pain_base",
        "//src/pain:pain_core",
        "//src/common:pain_common",
        "//protocols/pain/proto:cc_pain_proto",
        "//protocols/pain/proto:cc_pain_deva_proto",
        "//protocols/pain/proto:cc_pain_manusya_proto",
        "//protocols/pain/proto:cc_pain_asura_proto",
        "//protocols/pain/proto:cc_pain_errno_proto",
        "@nlohmann_json//:json",
        "@argparse",
        "@brpc",
        "@braft",
    ],
    visibility = ["//visibility:public"],
)
