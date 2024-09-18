set_project("pain")
set_languages("c++23")

set_version("1.0.0", {build = "%Y%m%d"})

add_rules("mode.debug", "mode.releasedbg")
add_rules("plugin.compile_commands.autoupdate", {outputdir = "build"})
set_policy("build.across_targets_in_parallel", false)
set_policy("check.auto_ignore_flags", false)

add_repositories("pain-repo pain-repo")

if is_mode("debug") then
    add_cxxflags("-fsanitize=address")
    add_ldflags("-fsanitize=address")
end

add_requires("protobuf-cpp 3.19.4", {configs = {zlib = true}})
add_requireconfs("**.protobuf-cpp", {version = "3.19.4", override=true, configs = {zlib = true}})

add_requires("brpc")
add_requires("fmt 10.1.1", {configs = {header_only = true}})
add_requireconfs("**.fmt", {override=true, version="10.1.1", configs = {header_only = true}})

add_requires("spdlog v1.14.1", {configs = {fmt_external = true, header_only = true}})
add_requireconfs("**.spdlog", {override=true, version="v1.14.1"})

add_requires("boost 1.81.0")
add_requireconfs("**.boost", {override=true, version="1.81.0"})

add_requires("gtest")
add_requires("argparse")
add_requires("nlohmann_json")
add_requires("opentelemetry-cpp")
add_requires("uuid_v4")

add_includedirs("include")
add_includedirs("src")

includes("src")
