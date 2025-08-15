#pragma once

#include "pain/proto/manusya.pb.h"

#define MANUSYA_SERVICE_METHOD(name)                                                                                   \
    void name(::google::protobuf::RpcController* controller,                                                           \
              const pain::proto::manusya::name##Request* request,                                                      \
              pain::proto::manusya::name##Response* response,                                                          \
              ::google::protobuf::Closure* done) override

namespace pain::manusya {
class ManusyaServiceImpl : public pain::proto::manusya::ManusyaService {
public:
    ManusyaServiceImpl();
    ~ManusyaServiceImpl() override = default;
    MANUSYA_SERVICE_METHOD(CreateChunk);
    MANUSYA_SERVICE_METHOD(AppendChunk);
    MANUSYA_SERVICE_METHOD(ListChunk);
    MANUSYA_SERVICE_METHOD(ReadChunk);
    MANUSYA_SERVICE_METHOD(QueryChunk);
    MANUSYA_SERVICE_METHOD(QueryAndSealChunk);
    MANUSYA_SERVICE_METHOD(RemoveChunk);
};

} // namespace pain::manusya

#undef MANUSYA_SERVICE_METHOD
