#include "pain/file_stream_impl.h"
#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <pain/base/macro.h>
#include <pain/base/plog.h>
#include "pain/controller.h"

#define FILE_STREAM_METHOD(name)                                                                                       \
    void FileStreamImpl::name(::google::protobuf::RpcController* controller,                                           \
                              [[maybe_unused]] const ::pain::proto::name##Request* request,                            \
                              [[maybe_unused]] ::pain::proto::name##Response* response,                                \
                              ::google::protobuf::Closure* done) // NOLINT(readability-non-const-parameter)

namespace pain {

FILE_STREAM_METHOD(Append) {
    SPAN("pain", span);
    [[maybe_unused]] pain::Controller* cntl = static_cast<pain::Controller*>(controller);
    PLOG_DEBUG(("desc", __func__)               //
               ("file_id", _file_id)            //
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
    [[maybe_unused]] pain::Controller* cntl = static_cast<pain::Controller*>(controller);
    PLOG_DEBUG(("desc", __func__));
    brpc::ClosureGuard done_guard(done);
}

} // namespace pain
