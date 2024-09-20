#pragma once

#include <butil/iobuf.h>
#include <butil/status.h>
#include <boost/intrusive_ptr.hpp>
#include "manusya/file_handle.h"

namespace pain::manusya {
class Store;
using StorePtr = boost::intrusive_ptr<Store>;
class Store {
public:
    Store() = default;
    virtual ~Store() = default;

    static StorePtr create();
    virtual FileHandlePtr open(const char *path) = 0;
    virtual butil::Status append(FileHandlePtr fh, uint64_t offset, butil::IOBuf &buf) = 0;
    virtual butil::Status read(FileHandlePtr fh, uint64_t offset, uint64_t size, butil::IOBuf *buf) = 0;
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