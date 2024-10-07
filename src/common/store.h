#pragma once

#include <atomic>
#include <memory>
#include <string_view>
#include <boost/intrusive_ptr.hpp>
#include "base/types.h"

namespace pain::common {
class Store;
using StorePtr = boost::intrusive_ptr<Store>;
// Store is a key-value store like redis
class Store {
public:
    class Iterator {
    public:
        virtual ~Iterator() = default;
        virtual bool valid() = 0;
        virtual std::string_view key() = 0;
        virtual std::string_view value() = 0;
        virtual void next() = 0;
    };

    virtual ~Store() = default;
    virtual Status hset(std::string_view key, std::string_view field, std::string_view value) = 0;
    virtual Status hget(std::string_view key, std::string_view field, std::string* value) = 0;
    virtual Status hdel(std::string_view key, std::string_view field) = 0;
    virtual Status hlen(std::string_view key, size_t* len) = 0;
    virtual std::shared_ptr<Iterator> hgetall(std::string_view key) = 0;
    virtual bool hexists(std::string_view key, std::string_view field) = 0;

private:
    std::atomic<int> _use_count;
    friend void intrusive_ptr_add_ref(Store* store) {
        store->_use_count++;
    }

    friend void intrusive_ptr_release(Store* store) {
        if (store->_use_count.fetch_sub(1) == 1) {
            delete store;
        }
    }
};

} // namespace pain::common
