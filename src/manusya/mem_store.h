#pragma once

#include <bthread/mutex.h>
#include "manusya/store.h"

namespace pain::manusya {

class MemStore : public Store {
public:
    MemStore() = default;
    ~MemStore() override = default;

protected:
    Future<Status> open(const char* path, int flags, FileHandlePtr* fh) override;
    Future<Status> append(FileHandlePtr fh, uint64_t offset, IOBuf buf) override;
    Future<Status> read(FileHandlePtr fh, uint64_t offset, uint64_t size, IOBuf* buf) override;
    Future<Status> seal(FileHandlePtr fh) override;
    Future<Status> size(FileHandlePtr fh, uint64_t* size) override;
    Future<Status> remove(const char* path) override;
    Future<Status> set_attr(FileHandlePtr fh, const char* key, const char* value) override;
    Future<Status> get_attr(FileHandlePtr fh, const char* key, std::string* value) override;
    Future<Status> list_attrs(FileHandlePtr fh, std::map<std::string, std::string>* attrs) override;
    void for_each(std::function<void(const char* path)> cb) override;

private:
    std::map<std::string, IOBuf> _files;
    std::map<std::string, std::map<std::string, std::string>> _attrs;
    mutable bthread::Mutex _mutex;

    friend class FileHandle;
};
} // namespace pain::manusya
