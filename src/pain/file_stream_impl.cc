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
    PLOG_DEBUG(("desc", __func__)               //
               ("uuid", _uuid.str())            //
               ("direct_io", cntl->direct_io()) //
               ("data_size", cntl->request_attachment().size()));

    // append to chunk
    // if _chunks is empty, create a new chunk
    // if the last chunk is full(64MB), create a new chunk
    // if append timeout, seal and new chunk
    // chunk = get_last_chunk();
    // foreach subchunk in chunk:
    //   subchunk->append(cntl->request_attachment());
    //   if timeout, seal and new chunk
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
