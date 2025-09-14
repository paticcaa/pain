#pragma once

#include <pain/base/object_id.h>
#include "manusya/chunk.h"
#include "manusya/store.h"

namespace pain::manusya {

class Bank {
public:
    Bank(StorePtr store) : _store(store){};
    ~Bank() = default;

    static Bank& instance();

    Status load();

    Status create_chunk(ChunkOptions options, uint32_t partition_id, ChunkPtr* chunk);

    Status get_chunk(ObjectId chunk_id, ChunkPtr* chunk);

    Status remove_chunk(ObjectId chunk_id);

    void list_chunk(ObjectId start, uint32_t limit, std::function<void(ObjectId chunk_id)> cb);

private:
    StorePtr _store;
    std::map<ObjectId, ChunkPtr> _chunks;
    mutable bthread::Mutex _mutex;
};

}; // namespace pain::manusya
