#pragma once

#include "manusya/chunk.h"
#include "manusya/store.h"

namespace pain::manusya {

class Bank {
public:
    Bank(StorePtr store) : _store(store){};
    ~Bank() = default;

    static Bank& instance();

    Status load();

    Status create_chunk(ChunkOptions options, ChunkPtr* chunk);

    Status get_chunk(UUID uuid, ChunkPtr* chunk);

    Status remove_chunk(UUID uuid);

    void list_chunk(UUID start, uint32_t limit, std::function<void(UUID uuid)> cb);

private:
    StorePtr _store;
    std::map<UUID, ChunkPtr> _chunks;
    mutable bthread::Mutex _mutex;
};

}; // namespace pain::manusya
