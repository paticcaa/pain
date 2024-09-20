#pragma once

#include <butil/iobuf.h>
#include <butil/status.h>
#include <cstdint>
#include <boost/intrusive_ptr.hpp>
#include "base/uuid.h"

namespace pain::manusya {

class Chunk;
using ChunkPtr = boost::intrusive_ptr<Chunk>;
class Chunk {
public:
    Chunk() = default;
    ~Chunk() = default;

    static ChunkPtr create();

    const base::UUID &uuid() const {
        return _uuid;
    }
    butil::Status append(butil::IOBuf &buf, uint64_t offset);
    butil::Status seal();
    butil::Status read(uint64_t offset, uint64_t size, butil::IOBuf *buf) const;
    uint64_t size() const {
        return _size;
    }

private:
    friend void intrusive_ptr_add_ref(Chunk *chunk) {
        chunk->_use_count++;
    }

    friend void intrusive_ptr_release(Chunk *chunk) {
        if (--chunk->_use_count == 0) {
            delete chunk;
        }
    }

    base::UUID _uuid;
    uint64_t _size = 0;
    int _use_count = 0;

    butil::IOBuf _data;
};

} // namespace pain::manusya