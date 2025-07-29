config_setting(
    name = "x86_debug_build",
    values = {
        "cpu": "x86",
        "compilation_mode": "dbg",
    },
)

config_setting(
    name = "x86_releasedbg_build",
    values = {
        "cpu": "x86",
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
