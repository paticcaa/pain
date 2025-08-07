package("spdk")
    set_description("The spdk package")

    add_urls("https://github.com/spdk/spdk.git")

    add_versions("v24.09", "19524ad45373e35d5fdc06f4e081444d87ceed80")

    -- sudo apt install libaio-dev
    -- sudo apt install uuid-dev
    -- sudo apt install nasm
    -- sudo apt install autoconf automake libtool
    -- sudo apt install libibverbs-dev
    -- sudo apt install librdmacm-dev
    -- sudo apt install patchelf

    add_deps("dpdk")
    add_deps("uuid", {system = true})
    add_deps("aio", {system = true})
    add_deps("ibverbs", {system = true})
    add_deps("rdmacm", {system = true})
    add_deps("openssl", {system = true})
    add_deps("lz4", {system = true})

    add_syslinks("pthread", "m", "dl", "rt")

    add_links("spdk_vhost","spdk_scsi","spdk_dma","spdk_bdev_aio","spdk_bdev_nvme","spdk_rdma_provider", "spdk_rdma_utils",
        "spdk_bdev","spdk_bdev_malloc","spdk_bdev_null","spdk_bdev_virtio","spdk_bdev_passthru",
        "spdk_virtio","spdk_vfio_user","spdk_lvol","spdk_blob","spdk_vmd","spdk_nvme","spdk_nvmf","spdk_sock","spdk_sock_posix",
        "spdk_thread","spdk_trace","spdk_rpc","spdk_jsonrpc","spdk_json","spdk_conf","spdk_util","spdk_log",
        "spdk_notify","spdk_accel","spdk_scheduler_dynamic",
        "spdk_env_dpdk","spdk_env_dpdk_rpc","spdk_event_iobuf",
        "spdk_event_accel","spdk_event_vmd","spdk_event_sock","spdk_event_scheduler",
        "spdk_event_vhost_scsi","spdk_event_vhost_blk",
        "spdk_event_scsi","spdk_event_bdev", "spdk_keyring", "spdk_event_keyring",
        "spdk_event","spdk_init", "isal", "isal_crypto")

    add_linkgroups("spdk_vhost","spdk_scsi","spdk_dma","spdk_bdev_aio","spdk_bdev_nvme","spdk_rdma_provider", "spdk_rdma_utils",
        "spdk_bdev","spdk_bdev_malloc","spdk_bdev_null","spdk_bdev_virtio","spdk_bdev_passthru",
        "spdk_virtio","spdk_vfio_user","spdk_lvol","spdk_blob","spdk_vmd","spdk_nvme","spdk_nvmf","spdk_sock","spdk_sock_posix",
        "spdk_thread","spdk_trace","spdk_rpc","spdk_jsonrpc","spdk_json","spdk_conf","spdk_util","spdk_log",
        "spdk_notify","spdk_accel","spdk_scheduler_dynamic",
        "spdk_env_dpdk","spdk_env_dpdk_rpc","spdk_event_iobuf",
        "spdk_event_accel","spdk_event_vmd","spdk_event_sock","spdk_event_scheduler",
        "spdk_event_vhost_scsi","spdk_event_vhost_blk",
        "spdk_event_scsi","spdk_event_bdev","spdk_keyring", "spdk_event_keyring",
        "spdk_event","spdk_init", "isal", "isal_crypto", {name = "spdk", whole = true})

    on_install(function (package)
        local configs = {}
        if package:debug() then
            table.insert(configs, "--enable-debug")
        end

        table.insert(configs, "--disable-tests")
        table.insert(configs, "--disable-unit-tests")
        table.insert(configs, "--without-nvme-cuse")
        table.insert(configs, "--with-crypto")
        table.insert(configs, "--with-rdma")
        -- table.insert(configs, "--max-numa-nodes=1")
        table.insert(configs, "--with-dpdk=" .. package:dep("dpdk"):installdir())

        import("package.tools.autoconf").install(package, configs)
        os.cp("include/spdk_internal/*.h", package:installdir("include/spdk_internal"))
        os.cp("isa-l/.libs/*.a", package:installdir("lib"))
        os.cp("isa-l-crypto/.libs/*.a", package:installdir("lib"))
        os.cp("isa-l/include/*.h", package:installdir("include/isa-l"))
        os.cp("isa-l-crypto/include/*.h", package:installdir("include/isa-l-crypto"))
        os.cp("scripts/*", package:installdir("scripts"))
        os.cp("python/*", package:installdir("python"))
        os.cp("$(buildir)/examples/*", package:installdir("examples"))
        os.cp("$(buildir)/fio/*", package:installdir("fio"))
    end)

    on_test(function (package)
        assert(package:has_cincludes("spdk/version.h"))
    end)
