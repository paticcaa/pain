#pragma once

#include "pain/proto/manusya.pb.h"

namespace pain::manusya {
class ManusyaServiceImpl : public pain::proto::manusya::ManusyaService {
public:
    ManusyaServiceImpl();
    virtual ~ManusyaServiceImpl() {}
    void CreateChunk(google::protobuf::RpcController* controller,
                     const pain::proto::manusya::CreateChunkRequest* request,
                     pain::proto::manusya::CreateChunkResponse* response,
                     google::protobuf::Closure* done) override;
    void AppendChunk(google::protobuf::RpcController* controller,
                     const pain::proto::manusya::AppendChunkRequest* request,
                     pain::proto::manusya::AppendChunkResponse* response,
                     google::protobuf::Closure* done) override;
    void ListChunk(google::protobuf::RpcController* controller,
                   const pain::proto::manusya::ListChunkRequest* request,
                   pain::proto::manusya::ListChunkResponse* response,
                   google::protobuf::Closure* done) override;

    void ReadChunk(google::protobuf::RpcController* controller,
                   const pain::proto::manusya::ReadChunkRequest* request,
                   pain::proto::manusya::ReadChunkResponse* response,
                   google::protobuf::Closure* done) override;
    void SealChunk(google::protobuf::RpcController* controller,
                   const pain::proto::manusya::SealChunkRequest* request,
                   pain::proto::manusya::SealChunkResponse* response,
                   google::protobuf::Closure* done) override;
    void RemoveChunk(google::protobuf::RpcController* controller,
                     const pain::proto::manusya::RemoveChunkRequest* request,
                     pain::proto::manusya::RemoveChunkResponse* response,
                     google::protobuf::Closure* done) override;
    void QueryChunk(google::protobuf::RpcController* controller,
                    const pain::proto::manusya::QueryChunkRequest* request,
                    pain::proto::manusya::QueryChunkResponse* response,
                    google::protobuf::Closure* done) override;
};

} // namespace pain::manusya
