#pragma once

#include "base/uuid.h"
#include "core/manusya.pb.h"
#include "manusya/chunk.h"

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

private:
    StorePtr _store;
    std::map<UUID, ChunkPtr> _chunks;
};

} // namespace pain::manusya
