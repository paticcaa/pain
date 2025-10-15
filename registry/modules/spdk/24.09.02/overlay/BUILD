package(
  default_visibility = ['//:__pkg__'],
)

load("@rules_foreign_cc//foreign_cc:configure.bzl", "configure_make")
load("//:spdk_libs.bzl", "create_library_targets", "all_libraries")

filegroup(
  name = 'all_srcs',
  srcs = glob(['**'], exclude = ['bazel-*/**']),
)

configure_make(
  name = 'spdk',
  lib_source = ':all_srcs',
  configure_in_place = True,
  configure_options = [
    "--without-shared",
    "--disable-tests",
    "--disable-unit-tests",
    "--without-nvme-cuse",
    "--with-crypto",
    "--with-rdma",
  ],
  out_static_libs = [lib + ".a" for lib in all_libraries],
  alwayslink = True,
  targets = ["-j", "install -j"],
  visibility = ['//visibility:public'],
  postfix_script = "echo \"rm pkgconfig and shared lib from $$INSTALLDIR start\" && rm -rf $$INSTALLDIR/lib/pkgconfig && rm -rf $$INSTALLDIR/lib/libspdk_*.so* && echo \"rm pkgconfig and shared lib from $$INSTALLDIR success\" && cp intel-ipsec-mb/lib/libIPSec_MB.a $$INSTALLDIR/lib/",
)

create_library_targets()
