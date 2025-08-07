package("dpdk")
    set_description("The dpdk package")

    add_urls("https://fast.dpdk.org/rel/dpdk-$(version).tar.xz")

    add_versions("23.11.2", "fbe4f971326653a2b9dc203dbfa990731e590b2e684e038b0513f45abf1a6cda")
    add_versions("24.03", "33ed973b3945af4f5923096ddca250b401dc80be3b5c6393b4e4fe43a1a6c2ad")

    add_deps("meson", "ninja")
    add_deps("zlib")
    add_deps("ibverbs", "crypto", "mana", "mlx4", "mlx5", "elf", "numa", {system = true})

    add_links("rte_node", "rte_graph", "rte_pipeline", "rte_table", "rte_pdump",
        "rte_port", "rte_fib", "rte_pdcp", "rte_ipsec", "rte_vhost", "rte_stack",
        "rte_security", "rte_sched", "rte_reorder", "rte_rib", "rte_mldev", "rte_regexdev",
        "rte_rawdev", "rte_power", "rte_pcapng", "rte_member", "rte_lpm", "rte_latencystats",
        "rte_jobstats", "rte_ip_frag", "rte_gso", "rte_gro", "rte_gpudev", "rte_dispatcher", "rte_eventdev",
        "rte_efd", "rte_dmadev", "rte_distributor", "rte_cryptodev", "rte_compressdev", "rte_cfgfile", "rte_bus_pci", "rte_bpf",
        "rte_bitratestats", "rte_bbdev", "rte_acl", "rte_timer", "rte_hash", "rte_metrics", "rte_cmdline", "rte_pci",
        "rte_ethdev", "rte_meter", "rte_net", "rte_mbuf", "rte_mempool", "rte_mempool_ring", "rte_rcu", "rte_ring", "rte_eal", "rte_telemetry",
        "rte_kvargs", "rte_log")
    add_linkgroups("rte_node", "rte_graph", "rte_pipeline", "rte_table", "rte_pdump",
        "rte_port", "rte_fib", "rte_pdcp", "rte_ipsec", "rte_vhost", "rte_stack",
        "rte_security", "rte_sched", "rte_reorder", "rte_rib", "rte_mldev", "rte_regexdev",
        "rte_rawdev", "rte_power", "rte_pcapng", "rte_member", "rte_lpm", "rte_latencystats",
        "rte_jobstats", "rte_ip_frag", "rte_gso", "rte_gro", "rte_gpudev", "rte_dispatcher", "rte_eventdev",
        "rte_efd", "rte_dmadev", "rte_distributor", "rte_cryptodev", "rte_compressdev", "rte_cfgfile", "rte_bpf",
        "rte_bitratestats", "rte_bbdev", "rte_acl", "rte_timer", "rte_hash", "rte_metrics", "rte_cmdline", "rte_bus_pci", "rte_pci",
        "rte_ethdev", "rte_meter", "rte_net", "rte_mbuf", "rte_mempool", "rte_mempool_ring", "rte_rcu", "rte_ring", "rte_eal", "rte_telemetry",
        "rte_kvargs", "rte_log", {name = "dpdk", whole = true})

    on_install(function (package)
        local configs = {}
        table.insert(configs, "-Dmax_numa_nodes=1")
        import("package.tools.meson").install(package, configs)
        -- rm *.so
        if not package:config("shared") then
            os.rm(package:installdir("lib/*.so*"))
        end
    end)

    on_test(function (package)
        assert(package:has_cincludes("rte_eal.h"))
    end)
