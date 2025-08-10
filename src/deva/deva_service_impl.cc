#include "deva/deva_service_impl.h"
#include <brpc/closure_guard.h>

#include "pain/proto/deva_store.pb.h"
#include "deva/bridge.h"
#include "deva/deva.h"
#include "deva/macro.h"

#define DEVA_SERVICE_METHOD(name)                                                                                      \
    void DevaServiceImpl::name(::google::protobuf::RpcController* controller,                                          \
                               [[maybe_unused]] const pain::proto::deva::name##Request* request,                       \
                               [[maybe_unused]] pain::proto::deva::name##Response* response,                           \
                               ::google::protobuf::Closure* done)

namespace pain::deva {

DevaServiceImpl::DevaServiceImpl() {}

DEVA_SERVICE_METHOD(OpenFile) {
    brpc::ClosureGuard done_guard(done);
    DEFINE_SPAN(span, controller);
}

DEVA_SERVICE_METHOD(CloseFile) {
    brpc::ClosureGuard done_guard(done);
    DEFINE_SPAN(span, controller);
}

DEVA_SERVICE_METHOD(RemoveFile) {
    brpc::ClosureGuard done_guard(done);
    DEFINE_SPAN(span, controller);
}

DEVA_SERVICE_METHOD(SealFile) {
    brpc::ClosureGuard done_guard(done);
    DEFINE_SPAN(span, controller);
}

DEVA_SERVICE_METHOD(NewChunk) {
    brpc::ClosureGuard done_guard(done);
    DEFINE_SPAN(span, controller);
}

DEVA_SERVICE_METHOD(CheckInChunk) {
    brpc::ClosureGuard done_guard(done);
    DEFINE_SPAN(span, controller);
}

DEVA_SERVICE_METHOD(SealChunk) {
    brpc::ClosureGuard done_guard(done);
    DEFINE_SPAN(span, controller);
}

DEVA_SERVICE_METHOD(SealAndNewChunk) {
    brpc::ClosureGuard done_guard(done);
    DEFINE_SPAN(span, controller);
}

} // namespace pain::deva

#undef DEVA_SERVICE_METHOD
