#include "pain/file_stream_impl.h"
#include <brpc/controller.h>
#include "pain/controller.h"
#include "base/plog.h"
#include "brpc/closure_guard.h"

namespace pain {

void FileStreamImpl::append(::google::protobuf::RpcController* controller,
                            const ::pain::core::AppendRequest* request,
                            ::pain::core::AppendResponse* response,
                            ::google::protobuf::Closure* done) {
    pain::Controller* cntl = static_cast<pain::Controller*>(controller);
    PLOG_DEBUG(("desc", __func__));
    brpc::ClosureGuard done_guard(done);
}

void FileStreamImpl::read(::google::protobuf::RpcController* controller,
                          const ::pain::core::ReadRequest* request,
                          ::pain::core::ReadResponse* response,
                          ::google::protobuf::Closure* done) {
    pain::Controller* cntl = static_cast<pain::Controller*>(controller);
    PLOG_DEBUG(("desc", __func__));
    brpc::ClosureGuard done_guard(done);
}

void FileStreamImpl::seal(::google::protobuf::RpcController* controller,
                          const ::pain::core::SealRequest* request,
                          ::pain::core::SealResponse* response,
                          ::google::protobuf::Closure* done) {
    pain::Controller* cntl = static_cast<pain::Controller*>(controller);
    PLOG_DEBUG(("desc", __func__));
    brpc::ClosureGuard done_guard(done);
}

void FileStreamImpl::close(::google::protobuf::RpcController* controller,
                           const ::pain::core::CloseRequest* request,
                           ::pain::core::CloseResponse* response,
                           ::google::protobuf::Closure* done) {
    pain::Controller* cntl = static_cast<pain::Controller*>(controller);
    PLOG_DEBUG(("desc", __func__));
    brpc::ClosureGuard done_guard(done);
}

} // namespace pain
