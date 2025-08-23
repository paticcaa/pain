#include <gflags/gflags.h>
#include <pain/base/plog.h>
#include <pain/base/scope_exit.h>
#include <pain/base/tracer.h>
#include <pain/pain.h>
#include <memory>
#include <spdlog/spdlog.h>

DEFINE_string(filesystem, "list://192.168.10.1:8001,192.168.10.2:8001,192.168.10.3:8001", "filesystem");

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    spdlog::set_level(spdlog::level::debug);
    pain::init_tracer("pain_demo");
    SCOPE_EXIT {
        PLOG_WARN(("desc", "pain_demo exit"));
        pain::cleanup_tracer();
    };

    pain::FileSystem* fs = nullptr;
    auto status = pain::FileSystem::create(FLAGS_filesystem.c_str(), &fs);
    if (!status.ok()) {
        std::cerr << "create filesystem failed: " << status.error_str() << std::endl;
        return 1;
    }
    std::unique_ptr<pain::FileSystem> fs_guard(fs);
    pain::FileStream* file = nullptr;
    status = fs->open("/hello.txt", O_CREAT | O_WRONLY, &file);
    if (!status.ok()) {
        std::cerr << "open file failed: " << status.error_str() << std::endl;
        return 1;
    }
    std::unique_ptr<pain::FileStream> file_guard(file);
    pain::proto::FileService::Stub stub(file);
    pain::Controller cntl;
    pain::proto::AppendRequest request;
    pain::proto::AppendResponse response;
    cntl.request_attachment().append("hello world");
    stub.Append(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        file->close();
        std::cerr << "append failed: " << cntl.ErrorText() << std::endl;
        return 1;
    }
    std::cout << "append success, offset: " << response.offset() << std::endl;
    file->close();
    return 0;
}
