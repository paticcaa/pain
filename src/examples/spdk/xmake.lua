target("spdk-nvme-hello-world")
    set_kind("binary")
    add_files("**.c")
    add_packages("spdk")
