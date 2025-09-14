#pragma once

#include <bthread/mutex.h>
#include <bthread/unstable.h>
#include <pain/base/future.h>
#include <pain/base/object_id.h>
#include <pain/base/tracer.h>
#include <pain/base/types.h>
#include <cstdint>
#include <boost/intrusive/set.hpp>
#include <boost/intrusive_ptr.hpp>
#include "manusya/file_handle.h"

namespace pain::manusya {

class Chunk;
using ChunkPtr = boost::intrusive_ptr<Chunk>;

struct ChunkOptions {};

enum class ChunkState {
    kInit = 0,
    kOpen = 1,
    kSealed = 2,
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

    static Status create(const ChunkOptions& options, StorePtr store, const ObjectId& chunk_id, ChunkPtr* chunk);

    const ObjectId& chunk_id() const {
        return _chunk_id;
    }
    Status append(const IOBuf& buf, uint64_t offset);
    Status query_and_seal(uint64_t* length);
    Status read(uint64_t offset, uint64_t size, IOBuf* buf) const;
    uint64_t size() const {
        return _size;
    }

    ChunkState state() const {
        return _state;
    }

    const ChunkOptions& options() const {
        return _options;
    }

    int use_count() const {
        return _use_count;
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

    ObjectId _chunk_id;
    uint64_t _size = 0;
    ChunkState _state = ChunkState::kInit;
    std::atomic<int> _use_count = 0;
    ChunkOptions _options;

    FileHandlePtr _fh;
    AppendRequestQueue _append_request_queue;
    StorePtr _store;
    mutable bthread::Mutex _mutex;
};

} // namespace pain::manusya
