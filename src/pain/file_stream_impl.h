#pragma once
#include <list>

#include "pain/chunk.h"
#include "pain/core/pain.pb.h"
#include "base/uuid.h"

namespace pain {

class Controller;
class AppendRequest;
class AppendResponse;
class ReadRequest;
class ReadResponse;
class FileStreamImpl : public pain::core::FileService {
public:
    void append(::google::protobuf::RpcController* controller,
                const ::pain::core::AppendRequest* request,
                ::pain::core::AppendResponse* response,
                ::google::protobuf::Closure* done) override;
    void read(::google::protobuf::RpcController* controller,
              const ::pain::core::ReadRequest* request,
              ::pain::core::ReadResponse* response,
              ::google::protobuf::Closure* done) override;
    void seal(::google::protobuf::RpcController* controller,
              const ::pain::core::SealRequest* request,
              ::pain::core::SealResponse* response,
              ::google::protobuf::Closure* done) override;
    void close(::google::protobuf::RpcController* controller,
               const ::pain::core::CloseRequest* request,
               ::pain::core::CloseResponse* response,
               ::google::protobuf::Closure* done) override;

private:
    ~FileStreamImpl() = default;
    UUID _uuid;
    std::list<Chunk> _chunks;
    friend class FileStream;
};

} // namespace pain
