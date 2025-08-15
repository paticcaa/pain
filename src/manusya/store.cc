#include "manusya/store.h"

#include <format>
#include <boost/assert.hpp>

#include "manusya/local_store.h"
#include "manusya/mem_store.h"

namespace pain::manusya {

StorePtr Store::create(const char* uri) {
    constexpr size_t local_prefix_len = 8;
    constexpr size_t memory_prefix_len = 9;
    if (strncmp(uri, "local://", local_prefix_len) == 0) {
        const char* data_path = uri + local_prefix_len;
        return StorePtr(new LocalStore(data_path));
    }

    if (strncmp(uri, "memory://", memory_prefix_len) == 0) {
        return StorePtr(new MemStore());
    }

    BOOST_ASSERT_MSG(false, std::format("unknown uri: {}", uri).c_str());
    return nullptr;
}

} // namespace pain::manusya
