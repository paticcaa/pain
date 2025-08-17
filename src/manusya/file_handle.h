#pragma once

#include <cstdint>

#include <boost/intrusive_ptr.hpp>

#include "base/types.h"
#include "manusya/store.h"

namespace pain::manusya {
class FileHandle;
using FileHandlePtr = boost::intrusive_ptr<FileHandle>;

class FileHandle {
public:
    FileHandle(StorePtr store) : _store(store) {
        BOOST_ASSERT_MSG(store != nullptr, "store is nullptr");
    }
    virtual ~FileHandle() = default;

    template <typename T>
    T* as() {
        return static_cast<T*>(this);
    }

    Future<Status> append(uint64_t offset, IOBuf buf) {
        return _store->append(this, offset, buf);
    }
    Future<Status> read(uint64_t offset, uint64_t size, IOBuf* buf) {
        return _store->read(this, offset, size, buf);
    }
    Future<Status> seal() {
        return _store->seal(this);
    }
    Future<Status> size(uint64_t* size) {
        return _store->size(this, size);
    }

    Future<Status> set_attr(const char* key, const char* value) {
        return _store->set_attr(this, key, value);
    }

    Future<Status> get_attr(const char* key, std::string* value) {
        return _store->get_attr(this, key, value);
    }

    Future<Status> list_attrs(std::map<std::string, std::string>* attrs) {
        return _store->list_attrs(this, attrs);
    }

    int use_count() const {
        return _use_count;
    }

private:
    StorePtr _store;

    friend void intrusive_ptr_add_ref(FileHandle* file_handle) {
        file_handle->_use_count++;
    }

    friend void intrusive_ptr_release(FileHandle* file_handle) {
        if (--file_handle->_use_count == 0) {
            delete file_handle;
        }
    }

    int _use_count = 0;
};

} // namespace pain::manusya
