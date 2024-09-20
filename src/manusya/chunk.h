#pragma once

#include <cstdint>
#include <boost/intrusive_ptr.hpp>
#include "base/types.h"
#include "base/uuid.h"
#include "manusya/file_handle.h"

namespace pain::manusya {

class Chunk;
using ChunkPtr = boost::intrusive_ptr<Chunk>;
class Chunk {
public:
    Chunk() = default;
    ~Chunk() = default;

    static Status create(StorePtr store, ChunkPtr* chunk);

    const UUID& uuid() const {
        return _uuid;
    }
    Status append(IOBuf& buf, uint64_t offset);
    Status seal();
    Status read(uint64_t offset, uint64_t size, IOBuf* buf) const;
    uint64_t size() const {
        return _size;
    }

private:
    friend void intrusive_ptr_add_ref(Chunk* chunk) {
        chunk->_use_count++;
    }

    friend void intrusive_ptr_release(Chunk* chunk) {
        if (--chunk->_use_count == 0) {
            delete chunk;
        }
    }

    UUID _uuid;
    uint64_t _size = 0;
    int _use_count = 0;

    FileHandlePtr _fh;
};

} // namespace pain::manusya
