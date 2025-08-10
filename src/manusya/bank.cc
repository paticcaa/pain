#include "manusya/bank.h"
#include "base/plog.h"

DEFINE_string(manusya_store, "memory://", "The path to store the data of manusya");

namespace pain::manusya {

Bank& Bank::instance() {
    static Bank bank(Store::create(FLAGS_manusya_store.c_str()));
    return bank;
}

Status Bank::load() {
    std::unique_lock lock(_mutex);
    _store->for_each([this](const char* path) mutable {
        PLOG_TRACE(("desc", "load chunk")("path", path));
        auto uuid = UUID::from_str_or_die(path);
        ChunkPtr chunk;
        auto status = Chunk::create({}, _store, uuid, &chunk);
        if (!status.ok()) {
            PLOG_ERROR(("desc", "failed to create chunk")("error", status.error_str()));
            return;
        }
        chunk->seal();
        _chunks[uuid] = chunk;
    });
    return Status::OK();
}

Status Bank::create_chunk(ChunkOptions options, ChunkPtr* chunk) {
    auto status = Chunk::create(options, _store, chunk);
    if (!status.ok()) {
        return status;
    }
    std::unique_lock lock(_mutex);
    _chunks[(*chunk)->uuid()] = *chunk;
    return Status::OK();
}

Status Bank::get_chunk(UUID uuid, ChunkPtr* chunk) {
    std::unique_lock lock(_mutex);
    auto it = _chunks.find(uuid);
    if (it == _chunks.end()) {
        return Status(ENOENT, "Chunk not found");
    }
    *chunk = it->second;
    return Status::OK();
}

Status Bank::remove_chunk(UUID uuid) {
    std::unique_lock lock(_mutex);
    auto it = _chunks.find(uuid);
    if (it == _chunks.end()) {
        return Status(ENOENT, "Chunk not found");
    }
    _chunks.erase(it);
    _store->remove(uuid.str().c_str()).get();
    return Status::OK();
}

void Bank::list_chunk(UUID start, uint32_t limit, std::function<void(UUID uuid)> cb) {
    std::unique_lock lock(_mutex);
    auto it = _chunks.lower_bound(start);
    for (uint32_t i = 0; i < limit && it != _chunks.end(); i++, it++) {
        try {
            cb(it->first);
        } catch (const std::exception& e) {
            PLOG_ERROR(("desc", "failed to list chunk")("error", e.what()));
        }
    }
}

}; // namespace pain::manusya
