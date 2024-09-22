#pragma once

#include <bthread/mutex.h>
#include <bthread/unstable.h>
#include <cstdint>
#include <boost/intrusive/set.hpp>
#include <boost/intrusive_ptr.hpp>
#include "base/future.h"
#include "base/tracer.h"
#include "base/types.h"
#include "base/uuid.h"
#include "manusya/file_handle.h"

namespace pain::manusya {

class Chunk;
using ChunkPtr = boost::intrusive_ptr<Chunk>;

struct ChunkOptions {
    bool append_out_of_order = false;
    bool digest = false;
};

enum class ChunkState {
    INIT = 0,
    OPEN = 1,
    SEALED = 2,
};

struct AppendRequest
    : public boost::intrusive::set_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>> {
    uint64_t offset = 0;
    IOBuf buf;
    uint64_t start = 0;
    uint64_t end = 0;
    ChunkPtr chunk;
    Promise<Status> promise;
    bthread_timer_t timer;
    std::shared_ptr<opentelemetry::trace::Span> span;
    int use_count = 0;

    friend bool operator<(const AppendRequest& a, const AppendRequest& b) {
        return a.offset < b.offset;
    }
    friend bool operator>(const AppendRequest& a, const AppendRequest& b) {
        return a.offset > b.offset;
    }
    friend bool operator==(const AppendRequest& a, const AppendRequest& b) {
        return a.offset == b.offset;
    }
    friend void intrusive_ptr_add_ref(AppendRequest* rq) {
        rq->use_count++;
    }
    friend void intrusive_ptr_release(AppendRequest* rq) {
        if (--rq->use_count == 0) {
            delete rq;
        }
    }
};

using AppendRequestPtr = boost::intrusive_ptr<AppendRequest>;
using AppendRequestQueue = boost::intrusive::set<AppendRequest, boost::intrusive::constant_time_size<false>>;

class Chunk {
public:
    Chunk() = default;
    ~Chunk() = default;

    static Status create(const ChunkOptions& options, StorePtr store, ChunkPtr* chunk);
    static Status create(const ChunkOptions& options, StorePtr store, const UUID& uuid, ChunkPtr* chunk);

    const UUID& uuid() const {
        return _uuid;
    }
    Status append(const IOBuf& buf, uint64_t offset);
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
    ChunkOptions _options;

    FileHandlePtr _fh;
    AppendRequestQueue _append_request_queue;
    StorePtr _store;
    mutable bthread::Mutex _mutex;
};

} // namespace pain::manusya
