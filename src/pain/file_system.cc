#include "pain/file_system.h"
#include "pain/file_stream.h"
#include "pain/file_stream_impl.h"

namespace pain {

std::shared_ptr<FileSystem> FileSystem::create(const char* uri) {
    return std::make_shared<FileSystem>();
}

std::shared_ptr<FileStream> FileSystem::open(const char* path, int flags) {
    auto fs = std::make_shared<FileStream>();
    fs->_impl = new FileStreamImpl();
    return fs;
}

} // namespace pain
