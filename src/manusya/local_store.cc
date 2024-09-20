#include "manusya/local_store.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <format>
#include <boost/assert.hpp>
#include "butil/iobuf.h"
#include "manusya/file_handle.h"
#include "manusya/store.h"

namespace pain::manusya {

class LocalFileHandle : public FileHandle {
public:
    LocalFileHandle(int fd, StorePtr store) :
        FileHandle(store),
        _fd(fd) {}

    virtual ~LocalFileHandle() {
        BOOST_ASSERT(_fd > 0);
        close(_fd);
    };

    int64_t handle() {
        return _fd;
    }

private:
    int64_t _fd = 0;
};

LocalStore::LocalStore(const char* data_path) :
    _data_path(data_path) {}

Status LocalStore::open(const char* path, int flags, FileHandlePtr* fh) {
    auto data_path = std::format("{}/{}", _data_path, path);
    int fd = ::open(path, flags | O_APPEND);
    if (fd < 0) {
        return Status(errno, "Failed to open file");
    }
    *fh = FileHandlePtr(new LocalFileHandle(fd, this));
    return Status::OK();
}

Status LocalStore::append(FileHandlePtr fh, uint64_t offset, IOBuf buf) {
    int fd = fh->as<LocalFileHandle>()->handle();

    auto buf_size = buf.size();
    auto nw = buf.cut_into_file_descriptor(fd, buf_size);
    if (nw < 0) {
        return Status(errno, "Failed to write to file");
    }

    BOOST_ASSERT(nw == buf_size);

    return Status::OK();
}

Status LocalStore::read(FileHandlePtr fh, uint64_t offset, uint64_t size, IOBuf* buf) {
    int fd = fh->as<LocalFileHandle>()->handle();

    butil::IOPortal iop;

    lseek(fd, offset, SEEK_SET);
    auto nr = iop.append_from_file_descriptor(fd, size);
    if (nr < 0) {
        return Status(errno, "Failed to read from file");
    }

    BOOST_ASSERT(nr == size);

    iop.swap(*buf);

    return Status::OK();
}

Status LocalStore::size(FileHandlePtr fh, uint64_t* size) {
    int fd = fh->as<LocalFileHandle>()->handle();
    struct stat st;
    if (fstat(fd, &st) < 0) {
        return Status(errno, "Failed to fstat");
    }

    *size = st.st_size;
    return Status::OK();
}

}; // namespace pain::manusya
