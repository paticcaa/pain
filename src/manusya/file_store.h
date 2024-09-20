#include "manusya/store.h"

namespace pain::manusya {

class FileStore : public Store {
public:
    FileStore() = default;
    virtual ~FileStore() = default;

    virtual FileHandlePtr open(const char *path) override;
    virtual Status append(FileHandlePtr fh, uint64_t offset, IOBuf &buf) override;
    virtual Status read(FileHandlePtr fh, uint64_t offset, uint64_t size, IOBuf *buf) override;
    virtual uint64_t size() override;
};

} // namespace pain::manusya
