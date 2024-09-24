#pragma once

#include "pain/core/deva.pb.h"

namespace pain::deva {

class DevaServiceImpl : public pain::core::deva::DevaService {
public:
    DevaServiceImpl();
    virtual ~DevaServiceImpl() {}
    void open(::google::protobuf::RpcController* controller,
              const pain::core::deva::OpenRequest* request,
              pain::core::deva::OpenResponse* response,
              ::google::protobuf::Closure* done) override;
    void close(::google::protobuf::RpcController* controller,
               const pain::core::deva::CloseRequest* request,
               pain::core::deva::CloseResponse* response,
               ::google::protobuf::Closure* done) override;
    void remove(::google::protobuf::RpcController* controller,
                const pain::core::deva::RemoveRequest* request,
                pain::core::deva::RemoveResponse* response,
                ::google::protobuf::Closure* done) override;
    void seal(::google::protobuf::RpcController* controller,
              const pain::core::deva::SealRequest* request,
              pain::core::deva::SealResponse* response,
              ::google::protobuf::Closure* done) override;
    void create_chunk(::google::protobuf::RpcController* controller,
                      const pain::core::deva::CreateChunkRequest* request,
                      pain::core::deva::CreateChunkResponse* response,
                      ::google::protobuf::Closure* done) override;
    void remove_chunk(::google::protobuf::RpcController* controller,
                      const pain::core::deva::RemoveChunkRequest* request,
                      pain::core::deva::RemoveChunkResponse* response,
                      ::google::protobuf::Closure* done) override;
    void seal_chunk(::google::protobuf::RpcController* controller,
                    const pain::core::deva::SealChunkRequest* request,
                    pain::core::deva::SealChunkResponse* response,
                    ::google::protobuf::Closure* done) override;
    void seal_and_new_chunk(::google::protobuf::RpcController* controller,
                            const pain::core::deva::SealAndNewChunkRequest* request,
                            pain::core::deva::SealAndNewChunkResponse* response,
                            ::google::protobuf::Closure* done) override;
};

} // namespace pain::deva
