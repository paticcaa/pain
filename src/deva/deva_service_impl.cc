#include "deva/deva_service_impl.h"
#include <brpc/closure_guard.h>

#include "pain/proto/deva_store.pb.h"
#include "deva/bridge.h"
#include "deva/deva.h"
#include "deva/macro.h"

namespace pain::deva {

DevaServiceImpl::DevaServiceImpl() {}

void DevaServiceImpl::Open(::google::protobuf::RpcController* controller,
                           const pain::proto::deva::OpenRequest* request,
                           pain::proto::deva::OpenResponse* response,
                           ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    DEFINE_SPAN(span, controller);
    proto::deva::store::OpenRequest req;
    proto::deva::store::OpenResponse resp;

    req.mutable_uuid()->CopyFrom(request->uuid());

    auto status = bridge<Deva, OpType::kOpen>(req, &resp).get();
    if (!status.ok()) {
        cntl->SetFailed(status.error_code(), "%s", status.error_cstr());
    }

    response->mutable_file_meta()->Swap(resp.mutable_file_meta());
}

void DevaServiceImpl::Close(::google::protobuf::RpcController* controller,
                            const pain::proto::deva::CloseRequest* request,
                            pain::proto::deva::CloseResponse* response,
                            ::google::protobuf::Closure* done) {}

void DevaServiceImpl::Remove(::google::protobuf::RpcController* controller,
                             const pain::proto::deva::RemoveRequest* request,
                             pain::proto::deva::RemoveResponse* response,
                             ::google::protobuf::Closure* done) {}

void DevaServiceImpl::Seal(::google::protobuf::RpcController* controller,
                           const pain::proto::deva::SealRequest* request,
                           pain::proto::deva::SealResponse* response,
                           ::google::protobuf::Closure* done) {}

void DevaServiceImpl::CreateChunk(::google::protobuf::RpcController* controller,
                                  const pain::proto::deva::CreateChunkRequest* request,
                                  pain::proto::deva::CreateChunkResponse* response,
                                  ::google::protobuf::Closure* done) {}

void DevaServiceImpl::RemoveChunk(::google::protobuf::RpcController* controller,
                                  const pain::proto::deva::RemoveChunkRequest* request,
                                  pain::proto::deva::RemoveChunkResponse* response,
                                  ::google::protobuf::Closure* done) {}

void DevaServiceImpl::SealChunk(::google::protobuf::RpcController* controller,
                                const pain::proto::deva::SealChunkRequest* request,
                                pain::proto::deva::SealChunkResponse* response,
                                ::google::protobuf::Closure* done) {}

void DevaServiceImpl::SealAndNewChunk(::google::protobuf::RpcController* controller,
                                      const pain::proto::deva::SealAndNewChunkRequest* request,
                                      pain::proto::deva::SealAndNewChunkResponse* response,
                                      ::google::protobuf::Closure* done) {}

} // namespace pain::deva
