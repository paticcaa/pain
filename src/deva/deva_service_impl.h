#pragma once

#include "pain/proto/deva.pb.h"

#define DEVA_SERVICE_METHOD(name)                                                                                      \
    void name(::google::protobuf::RpcController* controller,                                                           \
              const pain::proto::deva::name##Request* request,                                                         \
              pain::proto::deva::name##Response* response,                                                             \
              ::google::protobuf::Closure* done) override

namespace pain::deva {

class DevaServiceImpl : public pain::proto::deva::DevaService {
public:
    DevaServiceImpl();
    ~DevaServiceImpl() override = default;
    DEVA_SERVICE_METHOD(OpenFile);
    DEVA_SERVICE_METHOD(CloseFile);
    DEVA_SERVICE_METHOD(RemoveFile);
    DEVA_SERVICE_METHOD(SealFile);
    DEVA_SERVICE_METHOD(NewChunk);
    DEVA_SERVICE_METHOD(CheckInChunk);
    DEVA_SERVICE_METHOD(SealChunk);
    DEVA_SERVICE_METHOD(SealAndNewChunk);
};

} // namespace pain::deva

#undef DEVA_SERVICE_METHOD
