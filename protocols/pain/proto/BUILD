proto_library(
    name = "pain_common_proto",
    srcs = ["common.proto"],
    strip_import_prefix = "/protocols",
)

proto_library(
    name = "pain_deva_proto",
    srcs = ["deva.proto", "deva_group.proto", "deva_store.proto"],
    deps = [":pain_common_proto"],
    strip_import_prefix = "/protocols",
)

proto_library(
    name = "pain_manusya_proto",
    srcs = ["manusya.proto"],
    deps = [":pain_common_proto"],
    strip_import_prefix = "/protocols",
)

proto_library(
    name = "pain_asura_proto",
    srcs = ["asura.proto"],
    deps = [":pain_common_proto"],
    strip_import_prefix = "/protocols",
)

proto_library(
    name = "pain_proto",
    srcs = ["pain.proto"],
    deps = [":pain_common_proto"],
    strip_import_prefix = "/protocols",
)

proto_library(
    name = "pain_errno_proto",
    srcs = ["errno.proto"],
    strip_import_prefix = "/protocols",
)

cc_proto_library(
    name = "cc_pain_common_proto",
    deps = [
        ":pain_common_proto",
    ],
    visibility = ["//visibility:public"],
)

cc_proto_library(
    name = "cc_pain_deva_proto",
    deps = [":pain_deva_proto"],
    visibility = ["//visibility:public"],
)

cc_proto_library(
    name = "cc_pain_manusya_proto",
    deps = [":pain_manusya_proto"],
    visibility = ["//visibility:public"],
)

cc_proto_library(
    name = "cc_pain_asura_proto",
    deps = [":pain_asura_proto"],
    visibility = ["//visibility:public"],
)

cc_proto_library(
    name = "cc_pain_proto",
    deps = [":pain_proto"],
    visibility = ["//visibility:public"],
)

cc_proto_library(
    name = "cc_pain_errno_proto",
    deps = [":pain_errno_proto"],
    visibility = ["//visibility:public"],
)
