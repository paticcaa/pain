load('//:copts.bzl', 'PAIN_COPTS')

cc_binary(
    name = 'master',
    srcs = ['server.cc'],
    copts = PAIN_COPTS,
    deps = [
        '@brpc//:brpc',
        '//protocols:cc_pain_master_proto',
    ],
)
