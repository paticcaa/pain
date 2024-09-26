package("braft")
    set_description("braft is an industrial-grade C++ implementation of RAFT consensus algorithm and replicated state machine based on brpc")

    add_urls("https://github.com/baidu/braft.git")

    add_versions("1.1.3", "bc9fb052868560322a9b833c8179bb4d1a1f7dc5")

    add_deps("brpc")
    add_deps("protobuf-cpp")
    add_deps("leveldb", "gflags", "openssl", "zlib")
    add_deps("cmake")

    on_load("linux", function(package)
        package:addenv("PATH", "bin")
    end)

    on_install("linux", function (package)
        local configs = {}
        table.insert(configs, "-DCMAKE_BUILD_TYPE=" .. (package:debug() and "Debug" or "Release"))
        table.insert(configs, "-DWITH_DEBUG_SYMBOLS=" .. (package:debug() and "Debug" or "Release"))
        import("package.tools.cmake").install(package, configs)
        os.cp(path.join(package:buildir(), "tools/output/bin/braft_cli"), package:installdir("bin"))
        if not package:config("shared") then
            os.rm(package:installdir("lib/*.dylib"))
            os.rm(package:installdir("lib/*.so"))
        else
            os.rm(package:installdir("lib/*.a"))
        end
    end)

    on_test(function (package)
        assert(package:has_cxxincludes("braft/raft.h", {configs = {languages = "c++11"}}))
    end)
