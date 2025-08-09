load('//:copts.bzl', 'PAIN_COPTS', 'PAIN_LINKOPTS')

cc_binary(
    name = "asura",
    srcs = glob(["**/*.h", "**/*.cc"]),
    copts = PAIN_COPTS,
    linkopts = PAIN_LINKOPTS,
    deps = [
        "//protocols/pain/proto:cc_pain_asura_proto",
        "//src/base:pain_base",
        "//src/common:pain_common",
        "@uuid_v4",
    ],
)
