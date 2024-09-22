#pragma once

#include <memory>

/*
 * FileStream is a file stream abstraction that provides append, read and seal operations.
 * Example:
    #include "pain/pain.h"
    int main() {
        auto fs = pain::FileSystem::create("list://192.168.10.1:8001,192.168.10.2:8001,192.168.10.3:8001");
        auto file = fs->open("/tmp/hello.txt", O_CREAT | O_WRONLY);
        pain::core::FileService::Stub stub(file.get());
        pain::Controller cntl;
        pain::core::AppendRequest request;
        pain::core::AppendResponse response;
        stub.append(&cntl, &request, &response, nullptr);
        if (cntl.Failed()) {
            std::cerr << "append failed: " << cntl.ErrorText() << std::endl;
            return 1;
        }
        std::cout << "append success, offset: " << response.offset() << std::endl;
        return 0;
    }
 */

namespace pain {

class FileStream;
class FileSystem {
public:
    FileSystem() = default;
    FileSystem(const FileSystem&) = delete;
    FileSystem(FileSystem&&) = default;
    FileSystem& operator=(const FileSystem&) = delete;
    FileSystem& operator=(FileSystem&&) = default;

    static std::shared_ptr<FileSystem> create(const char* uri);
    std::shared_ptr<FileStream> open(const char* path, int flags);
};

} // namespace pain
