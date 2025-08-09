load("@rules_cc//cc:defs.bzl", "cc_import", "cc_library")

all_libraries = [
    "libspdk_vhost", "libspdk_scsi", "libspdk_dma", "libspdk_bdev_aio", "libspdk_bdev_nvme", "libspdk_rdma_provider", "libspdk_rdma_utils",
    "libspdk_bdev", "libspdk_bdev_malloc", "libspdk_bdev_null", "libspdk_bdev_virtio", "libspdk_bdev_passthru",
    "libspdk_virtio", "libspdk_vfio_user", "libspdk_lvol", "libspdk_blob", "libspdk_vmd", "libspdk_nvme", "libspdk_nvmf", "libspdk_sock", "libspdk_sock_posix",
    "libspdk_thread", "libspdk_trace", "libspdk_rpc", "libspdk_jsonrpc", "libspdk_json", "libspdk_conf", "libspdk_util", "libspdk_log",
    "libspdk_notify", "libspdk_accel", "libspdk_scheduler_dynamic",
    "libspdk_env_dpdk", "libspdk_env_dpdk_rpc", "libspdk_event_iobuf",
    "libspdk_event_accel", "libspdk_event_vmd", "libspdk_event_sock", "libspdk_event_scheduler",
    "libspdk_event_vhost_scsi", "libspdk_event_vhost_blk",
    "libspdk_event_scsi", "libspdk_event_bdev", "libspdk_keyring", "libspdk_event_keyring",
    "libspdk_event", "libspdk_init",
    "librte_bus_auxiliary",
    "librte_bus_pci",
    "librte_bus_vdev",
    "librte_cmdline",
    "librte_common_mlx5",
    "librte_common_qat",
    "librte_compressdev",
    "librte_crypto_ipsec_mb",
    "librte_crypto_mlx5",
    "librte_cryptodev",
    "librte_dmadev",
    "librte_eal",
    "librte_ethdev",
    "librte_hash",
    "librte_kvargs",
    "librte_log",
    "librte_mbuf",
    "librte_mempool",
    "librte_mempool_ring",
    "librte_meter",
    "librte_net",
    "librte_pci",
    "librte_power",
    "librte_rcu",
    "librte_reorder",
    "librte_ring",
    "librte_security",
    "librte_telemetry",
    "librte_timer",
    "librte_vhost",
    "libisal",
    "libisal_crypto",
    "libIPSec_MB",
]

dpdk_base_libraries = [
    "librte_vhost",
    "librte_security",
    "librte_reorder",
    "librte_power",
    "librte_dmadev",
    "librte_cryptodev",
    "librte_compressdev",
    "librte_timer",
    "librte_hash",
    "librte_cmdline",
    "librte_pci",
    "librte_ethdev",
    "librte_meter",
    "librte_net",
    "librte_mbuf",
    "librte_mempool",
    "librte_rcu",
    "librte_ring",
    "librte_eal",
    "librte_telemetry",
    "librte_kvargs",
    "librte_log",
    "libIPSec_MB",
]

dpdk_libraries = [
    "librte_bus_auxiliary",
    "librte_bus_pci",
    "librte_bus_vdev",
    "librte_common_mlx5",
    "librte_common_qat",
    "librte_mempool_ring",
    "librte_crypto_ipsec_mb",
    "librte_crypto_mlx5",
    "librte_vhost",
    "librte_security",
    "librte_reorder",
    "librte_power",
    "librte_dmadev",
    "librte_cryptodev",
    "librte_compressdev",
    "librte_timer",
    "librte_hash",
    "librte_cmdline",
    "librte_pci",
    "librte_ethdev",
    "librte_meter",
    "librte_net",
    "librte_mbuf",
    "librte_mempool",
    "librte_rcu",
    "librte_ring",
    "librte_eal",
    "librte_telemetry",
    "librte_kvargs",
    "librte_log",
]

spdk_libraries = [
    lib for lib in all_libraries if lib.startswith('libspdk')
]


    
def create_library_targets():
    native.filegroup(
        name = "_spdk_header_includes",
        srcs = [":spdk"],
        output_group = "include",
    )

    cc_library(
        name = "spdk_headers",
        hdrs = [
            ":_spdk_header_includes",
        ],
        includes = ["spdk/include"],
        visibility = ["//visibility:public"],
    )

    for lib in all_libraries:
        lib_name = lib + ".a"
        native.filegroup(
            name = "_" +lib,
            srcs = [":spdk"],
            output_group = lib_name,
        )

    cc_library(
        name = "dpdk_base_static",
        linkstatic = True,
        visibility = ["//visibility:public"],
        additional_linker_inputs = [
            ":_" + lib for lib in dpdk_base_libraries
        ],
        deps = [
            ":spdk_headers",
        ],
        linkopts = [
            "-Wl,--as-needed",
        ] + [
            "$(location :_" + lib + ")" for lib in dpdk_base_libraries
        ] + [
            "-Wl,--no-as-needed",
             "-pthread",
            "-lm",
            "-ldl",
            "-lnuma",
            "-luuid",
            "-laio",
            "-libverbs",
            "-lrdmacm",
            "-lcrypto",
            "-lssl",
            "-lmlx5",
            "-lmlx4",
            "-lmana",
            "-llz4",
        ],
    )

    cc_library(
        name = "dpdk_static",
        linkstatic = True,
        visibility = ["//visibility:public"],
        additional_linker_inputs = [
            ":_" + lib for lib in dpdk_libraries
        ] + [
            ":_libIPSec_MB",
        ],
        deps = [
            ":dpdk_base_static",
        ],
        linkopts = [
            "-Wl,--whole-archive",
        ] + [
            "$(location :_" + lib + ")" for lib in dpdk_libraries
        ] + [
            "-Wl,--no-whole-archive",
            "$(location :_libIPSec_MB)",
        ],
    )

    cc_library(
        name = "isal_static",
        linkstatic = True,
        visibility = ["//visibility:public"],
        additional_linker_inputs = [
            ":_libisal",
            ":_libisal_crypto",
        ],
        deps = [
            ":spdk_headers",
        ],
        linkopts = [
            "$(location :_libisal)",
            "$(location :_libisal_crypto)",
        ]
    )

    cc_library(
        name = "spdk_static",
        linkstatic = True,
        visibility = ["//visibility:public"],
        additional_linker_inputs = [
            ":_" + lib for lib in spdk_libraries
        ],
        deps = [
            ":dpdk_static",
            ":isal_static",
        ],
        linkopts = [
            "-Wl,--whole-archive",
        ] + [
            "$(location :_" + lib + ")" for lib in spdk_libraries
        ] + [
            "-Wl,--no-whole-archive"
        ],
    )
