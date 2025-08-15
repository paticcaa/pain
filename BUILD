load("@hedron_compile_commands//:refresh_compile_commands.bzl", "refresh_compile_commands")
load("@rules_pkg//pkg:install.bzl", "pkg_install")
load("@rules_pkg//pkg:mappings.bzl", "pkg_attributes", "pkg_files")

config_setting(
    name = "debug_build",
    values = {
        "compilation_mode": "dbg",
    },
)

config_setting(
    name = "releasedbg_build",
    values = {
        "compilation_mode": "opt",
    },
)

config_setting(
  name = 'compiler_gcc',
  flag_values = {
    '@bazel_tools//tools/cpp:compiler' : 'gcc',
  },
)

config_setting(
  name = 'compiler_clang',
  flag_values = {
    '@bazel_tools//tools/cpp:compiler' : 'clang',
  },
)


refresh_compile_commands(
    name = "refresh_compile_commands",
    targets = {
      "//src/...": "",
      "//protocols/...": "",
    },
)

pkg_files(
    name = "pain_binaries",
    srcs = [
      "//src/asura:asura",
      "//src/sad:sad",
      "//src/manusya:manusya",
      "//src/deva:deva",
    ],
    attributes = pkg_attributes(mode = "0755"),
    prefix = "bin",
)

pkg_install(
  name = "install",
  srcs = [":pain_binaries"],
  destdir = "output",
)
