#pragma once

#include <pain/status.h>
#include <memory>

/*
 * FileStream is a file stream abstraction that provides append, read and seal operations.
 * Example:
    #include "pain/pain.h"
    int main() {
        auto fs = pain::FileSystem::create("list://192.168.10.1:8001,192.168.10.2:8001,192.168.10.3:8001");
        auto file = fs->open("/tmp/hello.txt", O_CREAT | O_WRONLY);
        pain::proto::FileService::Stub stub(file.get());
        pain::Controller cntl;
        pain::proto::AppendRequest request;
        pain::proto::AppendResponse response;
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
class FileSystemImpl;
class FileSystem {
public:
    FileSystem() = default;
    FileSystem(const FileSystem&) = delete;
    FileSystem(FileSystem&&) = default;
    FileSystem& operator=(const FileSystem&) = delete;
    FileSystem& operator=(FileSystem&&) = default;
    ~FileSystem();

    static Status create(const char* uri, FileSystem** fs);
    Status open(const char* path, int flags, FileStream** file_stream);
    Status remove(const char* path);
    Status mkdir(const char* path);

private:
    FileSystemImpl* _impl;
};

} // namespace pain
