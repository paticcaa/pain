load('//:copts.bzl', 'PAIN_COPTS', 'PAIN_LINKOPTS')

cc_binary(
    name = "spdk_nvme_hello_world",
    srcs = ["main.c"],
    copts = PAIN_COPTS,
    linkopts = PAIN_LINKOPTS,
    deps = [
        "@spdk//:spdk_static",
    ],
)
