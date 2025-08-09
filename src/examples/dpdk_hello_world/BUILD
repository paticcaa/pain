load('//:copts.bzl', 'PAIN_COPTS', 'PAIN_LINKOPTS')

cc_binary(
    name = "dpdk_hello_world",
    srcs = ["main.cc"],
    copts = PAIN_COPTS,
    linkopts = PAIN_LINKOPTS,
    deps = [
        "@spdk//:dpdk_static",
    ],
)
