#include "pain/pain.h"
#include "spdlog/spdlog.h"

int main() {
    spdlog::set_level(spdlog::level::debug);
    auto fs = pain::FileSystem::create("list://192.168.10.1:8001,192.168.10.2:8001,192.168.10.3:8001");
    auto file = fs->open("/tmp/hello.txt", O_CREAT | O_WRONLY);
    pain::core::FileService::Stub stub(file.get());
    pain::Controller cntl;
    pain::core::AppendRequest request;
    pain::core::AppendResponse response;
    cntl.request_attachment().append("hello world");
    stub.append(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        std::cerr << "append failed: " << cntl.ErrorText() << std::endl;
        return 1;
    }
    std::cout << "append success, offset: " << response.offset() << std::endl;
    return 0;
}
