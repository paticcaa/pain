#pragma once

#include "pain/core/manusya.pb.h"

namespace pain::manusya {
class ManusyaServiceImpl : public pain::core::manusya::ManusyaService {
public:
    ManusyaServiceImpl();
    virtual ~ManusyaServiceImpl() {}
    void create_chunk(google::protobuf::RpcController* controller,
                      const pain::core::manusya::CreateChunkRequest* request,
                      pain::core::manusya::CreateChunkResponse* response,
                      google::protobuf::Closure* done) override;
    void append_chunk(google::protobuf::RpcController* controller,
                      const pain::core::manusya::AppendChunkRequest* request,
                      pain::core::manusya::AppendChunkResponse* response,
                      google::protobuf::Closure* done) override;
    void list_chunk(google::protobuf::RpcController* controller,
                    const pain::core::manusya::ListChunkRequest* request,
                    pain::core::manusya::ListChunkResponse* response,
                    google::protobuf::Closure* done) override;

    void read_chunk(google::protobuf::RpcController* controller,
                    const pain::core::manusya::ReadChunkRequest* request,
                    pain::core::manusya::ReadChunkResponse* response,
                    google::protobuf::Closure* done) override;
    void seal_chunk(google::protobuf::RpcController* controller,
                    const pain::core::manusya::SealChunkRequest* request,
                    pain::core::manusya::SealChunkResponse* response,
                    google::protobuf::Closure* done) override;
    void remove_chunk(google::protobuf::RpcController* controller,
                      const pain::core::manusya::RemoveChunkRequest* request,
                      pain::core::manusya::RemoveChunkResponse* response,
                      google::protobuf::Closure* done) override;
    void query_chunk(google::protobuf::RpcController* controller,
                     const pain::core::manusya::QueryChunkRequest* request,
                     pain::core::manusya::QueryChunkResponse* response,
                     google::protobuf::Closure* done) override;
};

} // namespace pain::manusya
