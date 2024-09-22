#include "manusya/store.h"

#include <format>
#include <boost/assert.hpp>

#include "manusya/local_store.h"
#include "manusya/mem_store.h"

namespace pain::manusya {

StorePtr Store::create(const char* uri) {
    if (strncmp(uri, "local://", 8) == 0) {
        const char* data_path = uri + 8;
        return StorePtr(new LocalStore(data_path));
    } else if (strncmp(uri, "memory://", 9) == 0) {
        return StorePtr(new MemStore());
    }

    BOOST_ASSERT_MSG(false, std::format("unknown uri: {}", uri).c_str());
    return nullptr;
}

} // namespace pain::manusya
