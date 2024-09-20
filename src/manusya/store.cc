#include "manusya/store.h"
#include "manusya/file_store.h"

namespace pain::manusya {

StorePtr Store::create(const char *uri) {
    if (strncmp(uri, "file://", 7) == 0) {
        return StorePtr(new FileStore());
    }
    return nullptr;
}

} // namespace pain::manusya