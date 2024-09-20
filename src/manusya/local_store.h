#include <cstdint>
#include "manusya/store.h"

namespace pain::manusya {

class LocalStore : public Store {
public:
    LocalStore(const char* data_path);
    virtual ~LocalStore() = default;

    virtual Status open(const char* path, int flags, FileHandlePtr* fh) override;
    virtual Status append(FileHandlePtr fh, uint64_t offset, IOBuf buf) override;
    virtual Status read(FileHandlePtr fh, uint64_t offset, uint64_t size, IOBuf* buf) override;
    virtual Status size(FileHandlePtr fh, uint64_t* size) override;

private:
    std::string _data_path;
};

} // namespace pain::manusya
