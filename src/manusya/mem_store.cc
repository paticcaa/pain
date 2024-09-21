#include "manusya/mem_store.h"
#include "manusya/file_handle.h"
#include "manusya/macro.h"

namespace pain::manusya {

class MemFileHandle : public FileHandle {
public:
    MemFileHandle(const char* path, StorePtr store) : FileHandle(store), _path(path){};

    virtual ~MemFileHandle() = default;

    const std::string& handle() {
        return _path;
    }

private:
    std::string _path;
};

Future<Status> MemStore::open(const char* path, int flags, FileHandlePtr* fh) {
    SPAN(span);
    std::ignore = flags;
    *fh = FileHandlePtr(new MemFileHandle(path, this));
    return make_ready_future(Status::OK());
}

Future<Status> MemStore::append(FileHandlePtr fh, uint64_t offset, IOBuf buf) {
    SPAN(span);
    auto path = fh->as<MemFileHandle>()->handle();
    auto& iobuf = _files[path];

    iobuf.append(buf);
    return make_ready_future(Status::OK());
}

Future<Status> MemStore::read(FileHandlePtr fh, uint64_t offset, uint64_t size, IOBuf* buf) {
    SPAN(span);
    auto path = fh->as<MemFileHandle>()->handle();
    auto& iobuf = _files[path];
    iobuf.append_to(buf, size, offset);
    return make_ready_future(Status::OK());
}

Future<Status> MemStore::seal(FileHandlePtr fh) {
    std::ignore = fh;
    return make_ready_future(Status::OK());
}

Future<Status> MemStore::size(FileHandlePtr fh, uint64_t* size) {
    SPAN(span);
    auto path = fh->as<MemFileHandle>()->handle();
    auto& iobuf = _files[path];
    *size = iobuf.size();
    return make_ready_future(Status::OK());
}

} // namespace pain::manusya
