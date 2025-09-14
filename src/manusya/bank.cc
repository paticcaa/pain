#include "manusya/bank.h"
#include <pain/base/plog.h>

DEFINE_string(manusya_store, "memory://", "The path to store the data of manusya");

namespace pain::manusya {

Bank& Bank::instance() {
    static Bank s_bank(Store::create(FLAGS_manusya_store.c_str()));
    return s_bank;
}

Status Bank::load() {
    std::unique_lock lock(_mutex);
    _store->for_each([this](const char* path) mutable {
        PLOG_TRACE(("desc", "load chunk")("path", path));
        auto id = ObjectId::from_str_or_die(path);
        ChunkPtr chunk;
        auto status = Chunk::create({}, _store, id, &chunk);
        if (!status.ok()) {
            PLOG_ERROR(("desc", "failed to create chunk")("error", status.error_str()));
            return;
        }
        uint64_t size = 0;
        // chunk should be sealed when loaded
        chunk->query_and_seal(&size);
        _chunks[id] = chunk;
    });
    return Status::OK();
}

Status Bank::create_chunk(ChunkOptions options, uint32_t partition_id, ChunkPtr* chunk) {
    if (chunk == nullptr) {
        return Status(EINVAL, "chunk is nullptr");
    }
    std::unique_lock lock(_mutex);
    auto status = Chunk::create(options, _store, ObjectId::generate(partition_id), chunk);
    if (!status.ok()) {
        return status;
    }
    _chunks[(*chunk)->chunk_id()] = *chunk;
    return Status::OK();
}

Status Bank::get_chunk(ObjectId chunk_id, ChunkPtr* chunk) {
    std::unique_lock lock(_mutex);
    auto it = _chunks.find(chunk_id);
    if (it == _chunks.end()) {
        return Status(ENOENT, "Chunk not found");
    }
    *chunk = it->second;
    return Status::OK();
}

Status Bank::remove_chunk(ObjectId chunk_id) {
    std::unique_lock lock(_mutex);
    auto it = _chunks.find(chunk_id);
    if (it == _chunks.end()) {
        return Status(ENOENT, "Chunk not found");
    }
    _chunks.erase(it);
    _store->remove(chunk_id.str().c_str()).get();
    return Status::OK();
}

void Bank::list_chunk(ObjectId start, uint32_t limit, std::function<void(ObjectId chunk_id)> cb) {
    std::unique_lock lock(_mutex);
    auto it = _chunks.lower_bound(start);
    for (uint32_t i = 0; i < limit && it != _chunks.end(); i++, it++) {
        try {
            cb(it->second->chunk_id());
        } catch (const std::exception& e) {
            PLOG_ERROR(("desc", "failed to list chunk")("error", e.what()));
        }
    }
}

}; // namespace pain::manusya
