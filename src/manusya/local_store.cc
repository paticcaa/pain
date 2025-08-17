#include "manusya/local_store.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/xattr.h>
#include <unistd.h>
#include <format>
#include <boost/assert.hpp>
#include "base/future.h"
#include "base/plog.h"
#include "butil/iobuf.h"
#include "manusya/file_handle.h"
#include "manusya/store.h"

namespace pain::manusya {

class LocalFileHandle : public FileHandle {
public:
    LocalFileHandle(int fd, StorePtr store) : FileHandle(store), _fd(fd) {}

    ~LocalFileHandle() override {
        BOOST_ASSERT(_fd > 0);
        close(_fd);
    };

    int64_t handle() const {
        return _fd;
    }

private:
    int64_t _fd = 0;
};

LocalStore::LocalStore(const char* data_path) : _data_path(data_path) {
    BOOST_ASSERT(data_path != nullptr);
    constexpr mode_t mode = 0774;
    int r = ::mkdir(data_path, mode);
    if (r < 0 && errno != EEXIST) {
        PLOG_ERROR(("desc", "failed to create data path")("path", data_path));
    }
}

Future<Status> LocalStore::open(const char* path, int flags, FileHandlePtr* fh) {
    if (path == nullptr) {
        return make_ready_future(Status(EINVAL, "path is nullptr"));
    }
    if (fh == nullptr) {
        return make_ready_future(Status(EINVAL, "fh is nullptr"));
    }
    auto data_path = std::format("{}/{}", _data_path, path);
    constexpr mode_t mode = 0666;
    int fd = ::open(data_path.c_str(), flags, mode);
    PLOG_DEBUG(("desc", "open file")("path", data_path)("fd", fd));
    if (fd < 0) {
        return make_ready_future(Status(errno, "failed to open file"));
    }
    *fh = FileHandlePtr(new LocalFileHandle(fd, this));
    return make_ready_future(Status::OK());
}

Future<Status> LocalStore::append(FileHandlePtr fh, uint64_t offset, IOBuf buf) {
    std::ignore = offset;
    if (fh == nullptr) {
        return make_ready_future(Status(EINVAL, "fh is nullptr"));
    }

    int fd = fh->as<LocalFileHandle>()->handle();

    auto buf_size = buf.size();
    auto nw = buf.cut_into_file_descriptor(fd, buf_size);
    PLOG_DEBUG(("desc", "append to file")("fd", fd)("nw", nw)("buf_size", buf_size));
    if (nw < 0) {
        return make_ready_future(Status(errno, "failed to write to file"));
    }

    BOOST_ASSERT(nw == buf_size);

    return make_ready_future(Status::OK());
}

Future<Status> LocalStore::read(FileHandlePtr fh, uint64_t offset, uint64_t size, IOBuf* buf) {
    if (fh == nullptr) {
        return make_ready_future(Status(EINVAL, "fh is nullptr"));
    }
    if (buf == nullptr) {
        return make_ready_future(Status(EINVAL, "buf is nullptr"));
    }

    int fd = fh->as<LocalFileHandle>()->handle();

    butil::IOPortal iop;

    auto r = lseek(fd, offset, SEEK_SET);
    if (r < 0) {
        return make_ready_future(Status(errno, "failed to lseek"));
    }

    auto nr = iop.append_from_file_descriptor(fd, size);
    if (nr < 0) {
        PLOG_ERROR(("desc", "failed to read from file")("error", errno)("fd", fd));
        return make_ready_future(Status(errno, "failed to read from file"));
    }

    if (nr != size) {
        return make_ready_future(Status(EINVAL, "invalid size"));
    }

    iop.swap(*buf);

    return make_ready_future(Status::OK());
}

Future<Status> LocalStore::seal(FileHandlePtr fh) {
    if (fh == nullptr) {
        return make_ready_future(Status(EINVAL, "fh is nullptr"));
    }

    int fd = fh->as<LocalFileHandle>()->handle();
    constexpr mode_t mode = 0444;
    int r = ::fchmod(fd, mode);
    if (r < 0) {
        return make_ready_future(Status(errno, "failed to fchmod"));
    }
    return make_ready_future(Status::OK());
}

Future<Status> LocalStore::size(FileHandlePtr fh, uint64_t* size) {
    if (fh == nullptr) {
        return make_ready_future(Status(EINVAL, "fh is nullptr"));
    }
    if (size == nullptr) {
        return make_ready_future(Status(EINVAL, "size is nullptr"));
    }

    int fd = fh->as<LocalFileHandle>()->handle();
    struct stat st;
    if (fstat(fd, &st) < 0) {
        return make_ready_future(Status(errno, "failed to fstat"));
    }

    *size = st.st_size;
    return make_ready_future(Status::OK());
}

Future<Status> LocalStore::remove(const char* path) {
    if (path == nullptr) {
        return make_ready_future(Status(EINVAL, "path is nullptr"));
    }

    auto data_path = std::format("{}/{}", _data_path, path);
    int r = ::unlink(data_path.c_str());
    if (r < 0) {
        return make_ready_future(Status(errno, "failed to unlink"));
    }
    return make_ready_future(Status::OK());
}

Future<Status> LocalStore::set_attr(FileHandlePtr fh, const char* key, const char* value) {
    if (fh == nullptr) {
        return make_ready_future(Status(EINVAL, "fh is nullptr"));
    }
    if (key == nullptr) {
        return make_ready_future(Status(EINVAL, "key is nullptr"));
    }
    if (value == nullptr) {
        return make_ready_future(Status(EINVAL, "value is nullptr"));
    }

    int fd = fh->as<LocalFileHandle>()->handle();
    int r = fsetxattr(fd, key, value, strlen(value), 0);
    if (r < 0) {
        return make_ready_future(Status(errno, "failed to fsetxattr"));
    }
    return make_ready_future(Status::OK());
}

Future<Status> LocalStore::get_attr(FileHandlePtr fh, const char* key, std::string* value) {
    if (fh == nullptr) {
        return make_ready_future(Status(EINVAL, "fh is nullptr"));
    }
    if (key == nullptr) {
        return make_ready_future(Status(EINVAL, "key is nullptr"));
    }
    if (value == nullptr) {
        return make_ready_future(Status(EINVAL, "value is nullptr"));
    }

    int fd = fh->as<LocalFileHandle>()->handle();
    constexpr size_t buf_size = 1024;
    char buf[buf_size];
    int r = fgetxattr(fd, key, buf, sizeof(buf));
    if (r < 0) {
        return make_ready_future(Status(errno, "failed to fgetxattr"));
    }
    *value = std::string(buf, r);
    return make_ready_future(Status::OK());
}

Future<Status> LocalStore::list_attrs(FileHandlePtr fh, std::map<std::string, std::string>* attrs) {
    if (fh == nullptr) {
        return make_ready_future(Status(EINVAL, "fh is nullptr"));
    }
    if (attrs == nullptr) {
        return make_ready_future(Status(EINVAL, "attrs is nullptr"));
    }

    int fd = fh->as<LocalFileHandle>()->handle();
    constexpr size_t buf_size = 1024;
    char buf[buf_size];
    int r = flistxattr(fd, buf, sizeof(buf));
    if (r < 0) {
        return make_ready_future(Status(errno, "failed to flistxattr"));
    }

    for (int i = 0; i < r; i++) {
        constexpr size_t key_size = 1024;
        char key[key_size];
        int n = fgetxattr(fd, buf + i, key, sizeof(key));
        if (n < 0) {
            return make_ready_future(Status(errno, "failed to fgetxattr"));
        }
        std::string value;
        auto status = get_attr(fh, key, &value).get();
        if (!status.ok()) {
            return make_ready_future(std::move(status));
        }
        (*attrs)[key] = value;
        i += strlen(buf + i);
    }

    return make_ready_future(Status::OK());
}

void LocalStore::for_each(std::function<void(const char* path)> cb) {
    DIR* dir = opendir(_data_path.c_str());
    if (dir == nullptr) {
        return;
    }

    struct dirent* entry = nullptr;
    while ((entry = readdir(dir)) != nullptr) {
        if (entry->d_type == DT_REG) {
            try {
                cb(entry->d_name);
            } catch (...) {
                continue;
            }
        }
    }
    closedir(dir);
}

}; // namespace pain::manusya
