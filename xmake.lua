set_project("pain")
set_languages("c++23")

set_version("1.0.0", {build = "%Y%m%d"})

add_rules("mode.debug", "mode.releasedbg")
add_rules("plugin.compile_commands.autoupdate", {outputdir = "build"})
set_policy("build.across_targets_in_parallel", false)
set_policy("check.auto_ignore_flags", false)
add_defines("SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_TRACE")

add_repositories("pain-repo pain-repo")

if is_mode("debug") then
    add_cxxflags("-fsanitize=address")
    add_ldflags("-fsanitize=address")
end

add_requires("argparse 3.1")
add_requires("brpc 1.10.0")
add_requires("boost 1.81.0")
add_requires("fmt 10.1.1", {configs = {header_only = true}})
add_requires("gtest 1.12.1")
add_requires("nlohmann_json v3.11.3")
add_requires("opentelemetry-cpp 1.16.1")
add_requires("protobuf-cpp 3.19.4", {configs = {zlib = true}})
add_requires("spdlog v1.14.1", {configs = {fmt_external = true, header_only = true}})
add_requires("uuid_v4 1.0.0")

add_requireconfs("**.boost", {override=true, version="1.81.0"})
add_requireconfs("**.fmt", {override=true, version="10.1.1", configs = {header_only = true}})
add_requireconfs("**.protobuf-cpp", {version = "3.19.4", override=true, configs = {zlib = true}})
add_requireconfs("**.spdlog", {override=true, version="v1.14.1"})


add_includedirs("include")
add_includedirs("src")

includes("src")
