#pragma once

#include <google/protobuf/service.h>

namespace pain {

class FileStreamImpl;
class FileStream : public google::protobuf::RpcChannel {
public:
    virtual ~FileStream();
    void CallMethod(const google::protobuf::MethodDescriptor* method,
                    google::protobuf::RpcController* controller,
                    const google::protobuf::Message* request,
                    google::protobuf::Message* response,
                    google::protobuf::Closure* done) override;

private:
    FileStreamImpl* _impl = nullptr;
    friend class FileSystem;
};

} // namespace pain
