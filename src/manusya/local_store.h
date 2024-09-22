#pragma once

#include <cstdint>
#include "manusya/store.h"

namespace pain::manusya {

class LocalStore : public Store {
public:
    LocalStore(const char* data_path);
    virtual ~LocalStore() = default;

    virtual Future<Status> open(const char* path, int flags, FileHandlePtr* fh) override;
    virtual Future<Status> append(FileHandlePtr fh, uint64_t offset, IOBuf buf) override;
    virtual Future<Status> read(FileHandlePtr fh, uint64_t offset, uint64_t size, IOBuf* buf) override;
    virtual Future<Status> seal(FileHandlePtr fh) override;
    virtual Future<Status> size(FileHandlePtr fh, uint64_t* size) override;
    virtual Future<Status> remove(const char* path) override;
    virtual void for_each(std::function<void(const char* path)> cb) override;

private:
    std::string _data_path;
};

} // namespace pain::manusya
