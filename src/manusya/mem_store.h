#pragma once

#include <bthread/mutex.h>
#include "manusya/store.h"

namespace pain::manusya {

class MemStore : public Store {
public:
    MemStore() = default;
    virtual ~MemStore() = default;

protected:
    virtual Future<Status> open(const char* path, int flags, FileHandlePtr* fh) override;
    virtual Future<Status> append(FileHandlePtr fh, uint64_t offset, IOBuf buf) override;
    virtual Future<Status> read(FileHandlePtr fh, uint64_t offset, uint64_t size, IOBuf* buf) override;
    virtual Future<Status> seal(FileHandlePtr fh) override;
    virtual Future<Status> size(FileHandlePtr fh, uint64_t* size) override;
    virtual Future<Status> remove(const char* path) override;
    virtual void for_each(std::function<void(const char* path)> cb) override;

private:
    std::map<std::string, IOBuf> _files;
    mutable bthread::Mutex _mutex;

    friend class FileHandle;
};
} // namespace pain::manusya
