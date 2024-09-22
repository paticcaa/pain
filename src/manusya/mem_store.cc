#include "manusya/mem_store.h"
#include <memory>
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
    std::unique_lock lock(_mutex);
    auto path = fh->as<MemFileHandle>()->handle();
    auto& iobuf = _files[path];

    iobuf.append(buf);
    return make_ready_future(Status::OK());
}

Future<Status> MemStore::read(FileHandlePtr fh, uint64_t offset, uint64_t size, IOBuf* buf) {
    SPAN(span);
    std::unique_lock lock(_mutex);
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
    std::unique_lock lock(_mutex);
    auto path = fh->as<MemFileHandle>()->handle();
    auto& iobuf = _files[path];
    *size = iobuf.size();
    return make_ready_future(Status::OK());
}

Future<Status> MemStore::remove(const char* path) {
    SPAN(span);
    std::unique_lock lock(_mutex);
    _files.erase(path);
    return make_ready_future(Status::OK());
}

Future<Status> MemStore::set_attr(FileHandlePtr fh, const char* key, const char* value) {
    SPAN(span);
    std::unique_lock lock(_mutex);
    auto path = fh->as<MemFileHandle>()->handle();
    _attrs[path][key] = value;
    return make_ready_future(Status::OK());
}

Future<Status> MemStore::get_attr(FileHandlePtr fh, const char* key, std::string* value) {
    SPAN(span);
    std::unique_lock lock(_mutex);
    auto path = fh->as<MemFileHandle>()->handle();
    auto it = _attrs[path].find(key);
    if (it == _attrs[path].end()) {
        return make_ready_future(Status(ENOENT, "attribute not found"));
    }
    *value = it->second;
    return make_ready_future(Status::OK());
}

Future<Status> MemStore::list_attrs(FileHandlePtr fh, std::map<std::string, std::string>* attrs) {
    SPAN(span);
    std::unique_lock lock(_mutex);
    auto path = fh->as<MemFileHandle>()->handle();
    *attrs = _attrs[path];
    return make_ready_future(Status::OK());
}

void MemStore::for_each(std::function<void(const char* path)> cb) {
    SPAN(span);
    std::unique_lock lock(_mutex);
    for (const auto& [path, _] : _files) {
        cb(path.c_str());
    }
}

} // namespace pain::manusya
