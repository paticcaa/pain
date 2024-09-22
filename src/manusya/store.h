#pragma once
#include <fcntl.h>
#include <unistd.h>
#include <boost/intrusive_ptr.hpp>
#include "base/future.h"
#include "base/types.h"

namespace pain::manusya {
class FileHandle;
using FileHandlePtr = boost::intrusive_ptr<FileHandle>;

class Store;
using StorePtr = boost::intrusive_ptr<Store>;
class Store {
public:
    Store() = default;
    virtual ~Store() = default;

    // support:
    //   local:///path/to/dir
    //   memory://
    static StorePtr create(const char* uri);
    virtual Future<Status> open(const char* path, int flags, FileHandlePtr* fh) = 0;
    virtual Future<Status> append(FileHandlePtr fh, uint64_t offset, IOBuf buf) = 0;
    virtual Future<Status> read(FileHandlePtr fh, uint64_t offset, uint64_t size, IOBuf* buf) = 0;
    virtual Future<Status> seal(FileHandlePtr fh) = 0;
    virtual Future<Status> size(FileHandlePtr fh, uint64_t* size) = 0;
    virtual Future<Status> remove(const char* path) = 0;
    virtual Future<Status> set_attr(FileHandlePtr fh, const char* key, const char* value) = 0;
    virtual Future<Status> get_attr(FileHandlePtr fh, const char* key, std::string* value) = 0;
    virtual Future<Status> list_attrs(FileHandlePtr fh, std::map<std::string, std::string>* attrs) = 0;
    virtual void for_each(std::function<void(const char* path)> cb) = 0;

private:
    friend void intrusive_ptr_add_ref(Store* store) {
        store->_use_count++;
    }

    friend void intrusive_ptr_release(Store* store) {
        if (store->_use_count.fetch_sub(1) == 1) {
            delete store;
        }
    }

    std::atomic<int> _use_count = 0;
};
} // namespace pain::manusya
