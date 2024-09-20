#pragma once

#include "manusya/store.h"

namespace pain::manusya {

class MemStore : public Store {
public:
    MemStore() = default;
    virtual ~MemStore() = default;

protected:
    virtual Status open(const char* path, int flags, FileHandlePtr* fh) override;
    virtual Status append(FileHandlePtr fh, uint64_t offset, IOBuf buf) override;
    virtual Status read(FileHandlePtr fh, uint64_t offset, uint64_t size, IOBuf* buf) override;
    virtual Status size(FileHandlePtr fh, uint64_t* size) override;

private:
    std::map<std::string, IOBuf> _files;

    friend class FileHandle;
};
} // namespace pain::manusya
