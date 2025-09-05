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

DevaServiceImpl::DevaServiceImpl(RsmPtr rsm) : _rsm(rsm) {}

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
        auto status = bridge<Deva, OpType::kCreateFile>(1, _rsm, create_request, &create_response).get();
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
        // readdir
        // get file info
        pain::proto::deva::store::GetFileInfoRequest get_file_info_request;
        pain::proto::deva::store::GetFileInfoResponse get_file_info_response;
        get_file_info_request.set_path(path);
        auto status = bridge<Deva, OpType::kGetFileInfo>(1, _rsm, get_file_info_request, &get_file_info_response).get();
        if (!status.ok()) {
            PLOG_ERROR(("desc", "failed to get file info")("error", status.error_str()));
        }
        response->mutable_file_info()->Swap(get_file_info_response.mutable_file_info());
        response->mutable_header()->set_status(0);
        response->mutable_header()->set_message("ok");
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
    auto status = bridge<Deva, OpType::kCreateDir>(1, _rsm, create_request, &create_response).get();
    if (!status.ok()) {
        PLOG_ERROR(("desc", "failed to create file")("error", status.error_str()));
        response->mutable_header()->set_status(status.error_code());
        response->mutable_header()->set_message(status.error_str());
        return;
    }
    response->mutable_file_info()->Swap(create_response.mutable_file_info());
    response->mutable_header()->set_status(0);
    response->mutable_header()->set_message("ok");
}

DEVA_SERVICE_METHOD(ReadDir) {
    brpc::ClosureGuard done_guard(done);
    DEFINE_SPAN(span, controller);
    auto& path = request->path();
    pain::proto::deva::store::ReadDirRequest read_dir_request;
    pain::proto::deva::store::ReadDirResponse read_dir_response;
    read_dir_request.set_path(path);
    auto status = bridge<Deva, OpType::kReadDir>(1, _rsm, read_dir_request, &read_dir_response).get();
    if (!status.ok()) {
        PLOG_ERROR(("desc", "failed to read dir")("error", status.error_str()));
        response->mutable_header()->set_status(status.error_code());
        response->mutable_header()->set_message(status.error_str());
        return;
    }
    response->mutable_entries()->Swap(read_dir_response.mutable_entries());
    response->mutable_header()->set_status(0);
    response->mutable_header()->set_message("ok");
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

DEVA_SERVICE_METHOD(ManusyaHeartbeat) {
    brpc::ClosureGuard done_guard(done);
    DEFINE_SPAN(span, controller);
    auto& manusya_registration = request->manusya_registration();
    pain::proto::deva::store::ManusyaHeartbeatRequest manusya_heartbeat_request;
    pain::proto::deva::store::ManusyaHeartbeatResponse manusya_heartbeat_response;

    manusya_heartbeat_request.mutable_manusya_registration()->CopyFrom(manusya_registration);

    auto status =
        bridge<Deva, OpType::kManusyaHeartbeat>(1, _rsm, manusya_heartbeat_request, &manusya_heartbeat_response).get();
    if (!status.ok()) {
        PLOG_ERROR(("desc", "failed to handle manusya heartbeat")("error", status.error_str()));
        response->mutable_header()->set_status(status.error_code());
        response->mutable_header()->set_message(status.error_str());
        return;
    }
    response->mutable_header()->set_status(0);
    response->mutable_header()->set_message("ok");
}

DEVA_SERVICE_METHOD(ListManusya) {
    brpc::ClosureGuard done_guard(done);
    DEFINE_SPAN(span, controller);
    pain::proto::deva::store::ListManusyaRequest list_manusya_request;
    pain::proto::deva::store::ListManusyaResponse list_manusya_response;
    auto status = bridge<Deva, OpType::kListManusya>(1, _rsm, list_manusya_request, &list_manusya_response).get();
    if (!status.ok()) {
        PLOG_ERROR(("desc", "failed to list manusya")("error", status.error_str()));
        response->mutable_header()->set_status(status.error_code());
        response->mutable_header()->set_message(status.error_str());
        return;
    }
    response->mutable_manusya_descriptors()->Swap(list_manusya_response.mutable_manusya_descriptors());
    response->mutable_header()->set_status(0);
    response->mutable_header()->set_message("ok");
}

} // namespace pain::deva

#undef DEVA_SERVICE_METHOD
