#pragma once

#include <brpc/server.h>
#include <butil/logging.h>
#include <gflags/gflags.h>
#include <json2pb/pb_to_json.h>
#include "protocols/errno.pb.h"
#include "protocols/master.pb.h"

// Your implementation of pain::proto::master::MasterService
namespace pain::master {
class MasterServiceImpl : public pain::proto::master::MasterService {
public:
    MasterServiceImpl() {}
    virtual ~MasterServiceImpl() {}
    void CreateFile(google::protobuf::RpcController* cntl_base,
                    const pain::proto::master::CreateFileRequest* request,
                    pain::proto::master::CreateFileResponse* response,
                    google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        LOG(INFO) << "Received request[log_id=" << cntl->log_id() << "] from " << cntl->remote_side() << ": "
                  << request->file_name() << " (attached=" << cntl->request_attachment() << ")";

        response->mutable_file_info()->set_file_id(request->file_name());
    }

    void GetFileInfo(google::protobuf::RpcController* cntl_base,
                     const pain::proto::master::GetFileInfoRequest* request,
                     pain::proto::master::GetFileInfoResponse* response,
                     google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        LOG(INFO) << "Received request[log_id=" << cntl->log_id() << "] from " << cntl->remote_side() << ": "
                  << request->file_id() << " (attached=" << cntl->request_attachment() << ")";

        auto it = _file_info_map.find(request->file_id());
        if (it == _file_info_map.end()) {
            response->mutable_header()->set_status(pain::proto::PAIN_ERR_NOT_FOUND);
            response->mutable_header()->set_message("File not found");
        } else {
            response->mutable_file_info()->CopyFrom(it->second);
        }
    }

private:
    std::map<std::string, pain::proto::FileInfo> _file_info_map;
};

} // namespace pain::master
