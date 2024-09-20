#include "manusya/mem_store.h"
#include "manusya/file_handle.h"

namespace pain::manusya {

class MemFileHandle : public FileHandle {
public:
    MemFileHandle(const char* path, StorePtr store) :
        FileHandle(store),
        _path(path){};

    virtual ~MemFileHandle() = default;

    const std::string& handle() {
        return _path;
    }

private:
    std::string _path;
};

Status MemStore::open(const char* path, int flags, FileHandlePtr* fh) {
    std::ignore = flags;
    *fh = FileHandlePtr(new MemFileHandle(path, this));
    return Status::OK();
}

Status MemStore::append(FileHandlePtr fh, uint64_t offset, IOBuf buf) {
    auto path = fh->as<MemFileHandle>()->handle();
    auto& iobuf = _files[path];

    iobuf.append(buf);
    return Status::OK();
}

Status MemStore::read(FileHandlePtr fh, uint64_t offset, uint64_t size, IOBuf* buf) {
    auto path = fh->as<MemFileHandle>()->handle();
    auto& iobuf = _files[path];
    iobuf.append_to(buf, size, offset);
    return Status::OK();
}

Status MemStore::size(FileHandlePtr fh, uint64_t* size) {
    auto path = fh->as<MemFileHandle>()->handle();
    auto& iobuf = _files[path];
    *size = iobuf.size();
    return Status::OK();
}

} // namespace pain::manusya
