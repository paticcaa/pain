#pragma once

#include <bthread/mutex.h>
#include <cstdint>
#include <boost/intrusive/set.hpp>
#include <boost/intrusive_ptr.hpp>
#include "base/future.h"
#include "base/types.h"
#include "base/uuid.h"
#include "manusya/file_handle.h"

namespace pain::manusya {

enum class ChunkState {
    INIT = 0,
    OPEN = 1,
    SEALED = 2,
};

struct AppendRequest : public boost::intrusive::set_base_hook<boost::intrusive::optimize_size<true>> {
    uint64_t offset;
    IOBuf buf;
    uint64_t start;
    uint64_t end;
    Promise<Status> promise;

    friend bool operator<(const AppendRequest& a, const AppendRequest& b) {
        return a.offset < b.offset;
    }
    friend bool operator>(const AppendRequest& a, const AppendRequest& b) {
        return a.offset > b.offset;
    }
    friend bool operator==(const AppendRequest& a, const AppendRequest& b) {
        return a.offset == b.offset;
    }
};

using AppendRequestQueue = boost::intrusive::set<AppendRequest>;

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

    ChunkState state() const {
        return _state;
    }

private:
    friend void intrusive_ptr_add_ref(Chunk* chunk) {
        chunk->_use_count++;
    }

    friend void intrusive_ptr_release(Chunk* chunk) {
        if (chunk->_use_count.fetch_sub(1) == 1) {
            delete chunk;
        }
    }

    UUID _uuid;
    uint64_t _size = 0;
    ChunkState _state = ChunkState::INIT;
    std::atomic<int> _use_count = 0;

    FileHandlePtr _fh;
    AppendRequestQueue _append_request_queue;
    mutable bthread::Mutex _mutex;
};

} // namespace pain::manusya
