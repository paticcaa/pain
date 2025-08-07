target("nvmf-tgt")
    set_kind("binary")
    add_files("**.c")
    add_packages("spdk")
