#pragma once

#include <boost/intrusive_ptr.hpp>
#include "base/types.h"
#include "manusya/file_handle.h"

namespace pain::manusya {

class Store;
using StorePtr = boost::intrusive_ptr<Store>;
class Store {
public:
    Store() = default;
    virtual ~Store() = default;

    // support:
    //   file://path/to/file
    //   memory
    //   bluefs
    static StorePtr create(const char *uri);
    virtual FileHandlePtr open(const char *path) = 0;
    virtual Status append(FileHandlePtr fh, uint64_t offset, IOBuf &buf) = 0;
    virtual Status read(FileHandlePtr fh, uint64_t offset, uint64_t size, IOBuf *buf) = 0;
    virtual uint64_t size() = 0;

private:
    friend void intrusive_ptr_add_ref(Store *store) {
        store->_use_count++;
    }

    friend void intrusive_ptr_release(Store *store) {
        if (--store->_use_count == 0) {
            delete store;
        }
    }

    int _use_count = 0;
};
} // namespace pain::manusya
