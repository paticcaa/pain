load('//:copts.bzl', 'PAIN_COPTS', 'PAIN_LINKOPTS')

cc_binary(
    name = 'master',
    srcs = glob(["**/*.h", "**/*.cc"]),
    copts = PAIN_COPTS,
    linkopts = PAIN_LINKOPTS,
    deps = [
        '@brpc//:brpc',
        '//protocols:cc_pain_master_proto',
        '//protocols:cc_pain_errno_proto',
    ],
)
