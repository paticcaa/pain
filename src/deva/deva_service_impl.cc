#include "deva/deva_service_impl.h"
#include <brpc/closure_guard.h>

#include "pain/core/deva_store.pb.h"
#include "deva/bridge.h"
#include "deva/deva.h"
#include "deva/macro.h"

namespace pain::deva {

DevaServiceImpl::DevaServiceImpl() {}

void DevaServiceImpl::open(::google::protobuf::RpcController* controller,
                           const pain::core::deva::OpenRequest* request,
                           pain::core::deva::OpenResponse* response,
                           ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    DEFINE_SPAN(span, controller);
    core::deva::store::OpenRequest req;
    core::deva::store::OpenResponse resp;

    req.mutable_uuid()->CopyFrom(request->uuid());

    auto status = bridge<Deva, OpType::kOpen>(req, &resp).get();
    if (!status.ok()) {
        cntl->SetFailed(status.error_code(), "%s", status.error_cstr());
    }

    response->mutable_file_meta()->Swap(resp.mutable_file_meta());
}

void DevaServiceImpl::close(::google::protobuf::RpcController* controller,
                            const pain::core::deva::CloseRequest* request,
                            pain::core::deva::CloseResponse* response,
                            ::google::protobuf::Closure* done) {}

void DevaServiceImpl::remove(::google::protobuf::RpcController* controller,
                             const pain::core::deva::RemoveRequest* request,
                             pain::core::deva::RemoveResponse* response,
                             ::google::protobuf::Closure* done) {}

void DevaServiceImpl::seal(::google::protobuf::RpcController* controller,
                           const pain::core::deva::SealRequest* request,
                           pain::core::deva::SealResponse* response,
                           ::google::protobuf::Closure* done) {}

void DevaServiceImpl::create_chunk(::google::protobuf::RpcController* controller,
                                   const pain::core::deva::CreateChunkRequest* request,
                                   pain::core::deva::CreateChunkResponse* response,
                                   ::google::protobuf::Closure* done) {}

void DevaServiceImpl::remove_chunk(::google::protobuf::RpcController* controller,
                                   const pain::core::deva::RemoveChunkRequest* request,
                                   pain::core::deva::RemoveChunkResponse* response,
                                   ::google::protobuf::Closure* done) {}

void DevaServiceImpl::seal_chunk(::google::protobuf::RpcController* controller,
                                 const pain::core::deva::SealChunkRequest* request,
                                 pain::core::deva::SealChunkResponse* response,
                                 ::google::protobuf::Closure* done) {}

void DevaServiceImpl::seal_and_new_chunk(::google::protobuf::RpcController* controller,
                                         const pain::core::deva::SealAndNewChunkRequest* request,
                                         pain::core::deva::SealAndNewChunkResponse* response,
                                         ::google::protobuf::Closure* done) {}

} // namespace pain::deva
