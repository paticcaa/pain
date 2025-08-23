#include "deva/deva_service_impl.h"
#include <brpc/closure_guard.h>

#include <pain/base/uuid.h>
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
    PLOG_DEBUG(("desc", "OpenFile")("request", request->DebugString()));
    auto& path = request->path();
    auto flags = request->flags();

    if ((flags & pain::proto::deva::OpenFlag::OPEN_CREATE) != 0) {
        pain::proto::deva::store::CreateFileRequest create_request;
        pain::proto::deva::store::CreateFileResponse create_response;
        auto file_id = UUID::generate();
        create_request.set_path(path);
        create_request.mutable_file_id()->set_high(file_id.high());
        create_request.mutable_file_id()->set_low(file_id.low());
        create_request.set_mode(0666); // NOLINT
        create_request.set_uid(0);
        create_request.set_gid(0);
        create_request.set_atime(butil::gettimeofday_us());
        create_request.set_mtime(butil::gettimeofday_us());
        create_request.set_ctime(butil::gettimeofday_us());
        auto status = bridge<Deva, OpType::kCreateFile>(create_request, &create_response).get();
        if (!status.ok()) {
            PLOG_ERROR(("desc", "failed to create file")("error", status.error_str()));
            response->mutable_header()->set_status(status.error_code());
            response->mutable_header()->set_message(status.error_str());
            return;
        }
        response->mutable_file_info()->Swap(create_response.mutable_file_info());
        response->mutable_header()->set_status(0);
        response->mutable_header()->set_message("ok");
    } else {
        // TODO: open file
    }

    response->mutable_header()->set_status(0);
    response->mutable_header()->set_message("ok");
}

DEVA_SERVICE_METHOD(CloseFile) {
    brpc::ClosureGuard done_guard(done);
    DEFINE_SPAN(span, controller);
}

DEVA_SERVICE_METHOD(RemoveFile) {
    brpc::ClosureGuard done_guard(done);
    DEFINE_SPAN(span, controller);
}

DEVA_SERVICE_METHOD(Mkdir) {
    brpc::ClosureGuard done_guard(done);
    DEFINE_SPAN(span, controller);
    auto& path = request->path();
    pain::proto::deva::store::CreateDirRequest create_request;
    pain::proto::deva::store::CreateDirResponse create_response;
    auto dir_id = UUID::generate();
    create_request.set_path(path);
    create_request.mutable_dir_id()->set_high(dir_id.high());
    create_request.mutable_dir_id()->set_low(dir_id.low());
    create_request.set_mode(0777); // NOLINT
    create_request.set_uid(0);
    create_request.set_gid(0);
    create_request.set_atime(butil::gettimeofday_us());
    create_request.set_mtime(butil::gettimeofday_us());
    create_request.set_ctime(butil::gettimeofday_us());
    auto status = bridge<Deva, OpType::kCreateDir>(create_request, &create_response).get();
    if (!status.ok()) {
        PLOG_ERROR(("desc", "failed to create file")("error", status.error_str()));
        response->mutable_header()->set_status(status.error_code());
        response->mutable_header()->set_message(status.error_str());
        return;
    }
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
