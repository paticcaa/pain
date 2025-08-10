load("@hedron_compile_commands//:refresh_compile_commands.bzl", "refresh_compile_commands")

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
      "//include/...": "",
      "//src/...": "",
      "//protocals/...": "",
      "//examples/...": "",
    },
)
