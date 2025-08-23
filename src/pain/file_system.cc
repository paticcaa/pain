#include <braft/route_table.h>
#include <brpc/channel.h>
#include <pain/file_stream.h>
#include <pain/file_stream_impl.h>
#include <pain/file_system.h>
#include <pain/proto/asura.pb.h>
#include <pain/proto/deva.pb.h>
#include <fmt/format.h>
#include "deva/sdk/rpc_client.h"

namespace pain {

class FileSystemImpl {
private:
    friend class FileSystem;
    std::string _cluster;
    std::vector<proto::asura::DevaServer> _deva_list;
};

FileSystem::~FileSystem() {
    delete _impl;
    _impl = nullptr;
}

Status FileSystem::create(const char* uri, FileSystem** fs) {
    if (fs == nullptr) {
        return Status(EINVAL, "fs is nullptr");
    }

    // get deva list
    ::brpc::ChannelOptions opts;
    const int default_timeout_ms = 20000;
    const int default_connect_timeout_ms = 2000;
    opts.timeout_ms = default_timeout_ms;
    opts.max_retry = 0;
    opts.connect_timeout_ms = default_connect_timeout_ms;
    brpc::Channel channel;

    if (strstr(uri, "://") != nullptr) {
        if (channel.Init(uri, "rr", &opts) != 0) {
            return Status(EINVAL, "connect to file system failed");
        }
    } else {
        if (channel.Init(uri, &opts) != 0) {
            return Status(EINVAL, "connect to file system failed");
        }
    }

    proto::asura::AsuraService::Stub stub(&channel);
    proto::asura::ListDevaRequest request;
    proto::asura::ListDevaResponse response;
    brpc::Controller cntl;
    stub.ListDeva(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        return Status(cntl.ErrorCode(), cntl.ErrorText());
    }

    if (response.header().status() != 0) {
        return Status(response.header().status(), response.header().message());
    }

    auto fs_impl = new FileSystemImpl();
    fs_impl->_cluster = uri;

    std::string deva_conf;
    for (const auto& deva : response.deva_servers()) {
        fs_impl->_deva_list.push_back(deva);
        deva_conf += fmt::format("{}:{},", deva.ip(), deva.port());
    }

    // TODO: we will fetch group name from asura
    PLOG_INFO(("desc", "update deva configuration")("deva_conf", deva_conf));
    braft::rtb::update_configuration("default", deva_conf);

    *fs = new FileSystem();
    (*fs)->_impl = fs_impl;
    return Status::OK();
}

Status FileSystem::open(const char* path, int flags, FileStream** file_stream) {
    SPAN("pain", open_file_span);
    proto::deva::OpenFileRequest request;
    proto::deva::OpenFileResponse response;
    request.set_path(path);
    uint32_t deva_flags = 0;
    if ((flags & O_CREAT) != 0) {
        deva_flags |= proto::deva::OpenFlag::OPEN_CREATE;
    }
    if ((flags & O_RDONLY) != 0) {
        deva_flags |= proto::deva::OpenFlag::OPEN_READ;
    }
    if ((flags & O_WRONLY) != 0) {
        deva_flags |= proto::deva::OpenFlag::OPEN_APPEND;
    }
    request.set_flags(deva_flags);
    auto status = deva::call_rpc("default", &proto::deva::DevaService::Stub::OpenFile, &request, &response);
    if (!status.ok()) {
        return Status(status.error_code(), status.error_str());
    }

    PLOG_DEBUG(("desc", "open file")("file_info", response.file_info().DebugString()));

    auto file_info = response.file_info();
    auto file_stream_impl = new FileStreamImpl();
    file_stream_impl->_file_info = file_info;
    UUID uuid(file_info.file_id().high(), file_info.file_id().low());
    file_stream_impl->_file_id = uuid.str();

    auto fs = new FileStream();
    fs->_impl = file_stream_impl;
    *file_stream = fs;
    return Status::OK();
}

Status FileSystem::remove(const char* path) {
    proto::deva::RemoveFileRequest request;
    proto::deva::RemoveFileResponse response;
    request.set_path(path);
    auto status = deva::call_rpc("default", &proto::deva::DevaService::Stub::RemoveFile, &request, &response);
    if (!status.ok()) {
        return Status(status.error_code(), status.error_str());
    }
    return Status::OK();
}

Status FileSystem::mkdir(const char* path) {
    proto::deva::MkdirRequest request;
    proto::deva::MkdirResponse response;
    request.set_path(path);
    auto status = deva::call_rpc("default", &proto::deva::DevaService::Stub::Mkdir, &request, &response);
    if (!status.ok()) {
        return Status(status.error_code(), status.error_str());
    }
    return Status::OK();
}

} // namespace pain
