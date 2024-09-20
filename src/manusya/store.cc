#include "manusya/store.h"
#include "manusya/local_store.h"
#include "manusya/mem_store.h"

namespace pain::manusya {

StorePtr Store::create(const char* uri) {
    if (strncmp(uri, "local://", 7) == 0) {
        const char* data_path = uri + 7;
        return StorePtr(new LocalStore(data_path));
    } else if (strncmp(uri, "memory", 6) == 0) {
        return StorePtr(new MemStore());
    }
    return nullptr;
}

} // namespace pain::manusya
