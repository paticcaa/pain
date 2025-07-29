proto_library(
    name = "pain_common_proto",
    srcs = ["common.proto"],
)

proto_library(
    name = "pain_master_proto",
    srcs = ["master.proto"],
    deps = [":pain_common_proto"],
)

cc_proto_library(
    name = "cc_pain_common_proto",
    deps = [":pain_common_proto"],
    visibility = ["//visibility:public"],
)

cc_proto_library(
    name = "cc_pain_master_proto",
    deps = [":pain_master_proto"],
    visibility = ["//visibility:public"],
)
