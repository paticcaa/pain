#include "manusya/chunk.h"
#include <cerrno>

namespace pain::manusya {

Status Chunk::append(IOBuf& buf, uint64_t offset) {
    if (offset != _size) {
        return Status(EINVAL, "Invalid offset");
    }
    _size += buf.size();
    _fh->append(offset, buf);
    return Status::OK();
}

Status Chunk::seal() {
    return Status::OK();
}

Status Chunk::read(uint64_t offset, uint64_t size, IOBuf* buf) const {
    if (offset + size > _size) {
        return Status(EINVAL, "Invalid offset or size");
    }
    _fh->read(offset, size, buf);
    return Status::OK();
}

Status Chunk::create(StorePtr store, ChunkPtr* chunk) {
    static UUIDv4::UUIDGenerator<std::mt19937_64> uuid_gen;
    auto c = ChunkPtr(new Chunk());
    c->_uuid = uuid_gen.getUUID();
    auto status = store->open(c->_uuid.str().c_str(), O_CREAT | O_RDWR, &c->_fh);

    if (!status.ok()) {
        return status;
    }

    *chunk = c;
    return Status::OK();
}

} // namespace pain::manusya
