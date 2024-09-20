#include "manusya/file_store.h"

namespace pain::manusya {

using Status = Status;

FileHandlePtr FileStore::open(const char *path) {
    return nullptr;
}

Status FileStore::append(FileHandlePtr fh, uint64_t offset, IOBuf &buf) {
    return Status();
}

Status FileStore::read(FileHandlePtr fh, uint64_t offset, uint64_t size, IOBuf *buf) {
    return Status();
}

uint64_t FileStore::size() {
    return 0;
}

}; // namespace pain::manusya
