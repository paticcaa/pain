#include "pain/file_stream_impl.h"
#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include "pain/controller.h"
#include "base/plog.h"

#define FILE_STREAM_METHOD(name)                                                                                       \
    void FileStreamImpl::name(::google::protobuf::RpcController* controller,                                           \
                              [[maybe_unused]] const ::pain::proto::name##Request* request,                            \
                              [[maybe_unused]] ::pain::proto::name##Response* response,                                \
                              ::google::protobuf::Closure* done)

namespace pain {

FILE_STREAM_METHOD(Append) {
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

FILE_STREAM_METHOD(Read) {
    pain::Controller* cntl = static_cast<pain::Controller*>(controller);
    PLOG_DEBUG(("desc", __func__));
    brpc::ClosureGuard done_guard(done);
}

FILE_STREAM_METHOD(Seal) {
    pain::Controller* cntl = static_cast<pain::Controller*>(controller);
    PLOG_DEBUG(("desc", __func__));
    brpc::ClosureGuard done_guard(done);
}

FILE_STREAM_METHOD(Close) {
    pain::Controller* cntl = static_cast<pain::Controller*>(controller);
    PLOG_DEBUG(("desc", __func__));
    brpc::ClosureGuard done_guard(done);
}

} // namespace pain
