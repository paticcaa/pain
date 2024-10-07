#pragma once

#include <atomic>
#include <string>
#include <string_view>
#include <boost/intrusive_ptr.hpp>
#include "base/types.h"

namespace pain::deva {

class Container {
public:
    virtual ~Container() = default;
    virtual Status save_snapshot(std::string_view path, std::vector<std::string>* files) = 0;
    virtual Status load_snapshot(std::string_view path) = 0;

private:
    std::atomic<int> _use_count = 0;

    friend void intrusive_ptr_add_ref(Container* node) {
        ++node->_use_count;
    }

    friend void intrusive_ptr_release(Container* node) {
        if (node->_use_count.fetch_sub(1) == 1) {
            delete node;
        }
    }
};

using ContainerPtr = boost::intrusive_ptr<Container>;

} // namespace pain::deva
