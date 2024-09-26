target("deva")
    set_kind("binary")
    add_files("**.cc|test/**.cc")
    add_deps("pain_base")
    add_deps("pain_core")
    add_packages("uuid_v4")
    add_packages("braft")

includes("test")
