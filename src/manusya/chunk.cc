#include "manusya/chunk.h"
#include <cerrno>

namespace pain::manusya {
using Status = butil::Status;

Status Chunk::append(butil::IOBuf &buf, uint64_t offset) {
  if (offset != _size) {
    return Status(EINVAL, "Invalid offset");
  }
  _size += buf.size();
  _data.append(buf);
  return Status::OK();
}

Status Chunk::seal() { return Status::OK(); }

Status Chunk::read(uint64_t offset, uint64_t size, butil::IOBuf *buf) const {
  if (offset + size > _size) {
    return Status(EINVAL, "Invalid offset or size");
  }
  _data.append_to(buf, size, offset);
  return Status::OK();
}

ChunkPtr Chunk::create() {
  static UUIDv4::UUIDGenerator<std::mt19937_64> uuid_gen;
  auto c = ChunkPtr(new Chunk());
  c->_uuid = uuid_gen.getUUID();
  return c;
}

} // namespace pain::manusya