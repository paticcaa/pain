#pragma once

#include <cstdint>

#include <boost/intrusive_ptr.hpp>

namespace pain::manusya {
class FileHandle;
using FileHandlePtr = boost::intrusive_ptr<FileHandle>;

class FileHandle {
public:
    virtual ~FileHandle() = default;
    virtual uint64_t handle() = 0;

private:
    friend void intrusive_ptr_add_ref(FileHandle *file_handle) {
        file_handle->_use_count++;
    }

    friend void intrusive_ptr_release(FileHandle *file_handle) {
        if (--file_handle->_use_count == 0) {
            delete file_handle;
        }
    }

    int _use_count = 0;
};

} // namespace pain::manusya