package(
  default_visibility = ['//:__pkg__'],
)

load('@rules_foreign_cc//foreign_cc:defs.bzl', 'cmake')

filegroup(
  name = 'all_srcs',
  srcs = glob(['**'], exclude = ['bazel-*/**']),
)

cmake(
  name = 'uuid_v4',
  lib_source = ':all_srcs',
  out_headers_only = True,
  visibility = ['//visibility:public'],
)
