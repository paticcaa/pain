#pragma once

#include "base/macro.h"

#define ASURA_SPAN(...) DEFINE_SPAN("asura", __VA_ARGS__)

#define ASURA_RPC_ENTRY(name)                                                                                          \
    void name(::google::protobuf::RpcController* controller,                                                           \
              const pain::proto::asura::name##Request* request,                                                        \
              pain::proto::asura::name##Response* response,                                                            \
              ::google::protobuf::Closure* done) override;
