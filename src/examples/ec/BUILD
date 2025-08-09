load('//:copts.bzl', 'PAIN_COPTS', 'PAIN_LINKOPTS')

cc_binary(
    name = "ec_demo",
    srcs = ["ec_demo.c"],
    copts = PAIN_COPTS,
    linkopts = PAIN_LINKOPTS,
    deps = [
        "@spdk//:isal_static",
    ],
)
