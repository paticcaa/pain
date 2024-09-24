#include "manusya/manusya_service_impl.h"

#include <brpc/controller.h>

#include "base/plog.h"
#include "base/tracer.h"
#include "butil/endpoint.h"
#include "manusya/bank.h"
#include "manusya/chunk.h"
#include "manusya/macro.h"

namespace pain::manusya {

ManusyaServiceImpl::ManusyaServiceImpl() {}

void ManusyaServiceImpl::create_chunk(google::protobuf::RpcController* controller,
                                      const pain::core::manusya::CreateChunkRequest* request,
                                      pain::core::manusya::CreateChunkResponse* response,
                                      google::protobuf::Closure* done) {
    DEFINE_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);

    PLOG_DEBUG(("desc", __func__)                                                //
               ("remote_side", butil::endpoint2str(cntl->remote_side()).c_str()) //
               ("attached", cntl->request_attachment().size()));

    ChunkOptions options;
    options.append_out_of_order = request->chunk_options().append_out_of_order();
    options.digest = request->chunk_options().digest();

    ChunkPtr chunk;
    auto status = Bank::instance().create_chunk(options, &chunk);
    if (!status.ok()) {
        PLOG_ERROR(("desc", "failed to create chunk")("error", status.error_str()));
        cntl->SetFailed(status.error_code(), "%s", status.error_cstr());
        return;
    }

    response->mutable_uuid()->set_low(chunk->uuid().low());
    response->mutable_uuid()->set_high(chunk->uuid().high());
}

void ManusyaServiceImpl::append_chunk(google::protobuf::RpcController* controller,
                                      const pain::core::manusya::AppendChunkRequest* request,
                                      pain::core::manusya::AppendChunkResponse* response,
                                      google::protobuf::Closure* done) {
    DEFINE_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);
    UUID uuid(request->uuid().high(), request->uuid().low());
    span->SetAttribute("chunk", uuid.str());
    PLOG_DEBUG(("desc", __func__)                                                //
               ("remote_side", butil::endpoint2str(cntl->remote_side()).c_str()) //
               ("chunk", uuid.str())                                             //
               ("offset", request->offset())                                     //
               ("attached", cntl->request_attachment().size()));

    ChunkPtr chunk;
    auto status = Bank::instance().get_chunk(uuid, &chunk);
    if (!status.ok()) {
        PLOG_ERROR(("desc", "chunk not found")("uuid", uuid.str()));
        cntl->SetFailed(ENOENT, "Chunk not found");
        return;
    }

    auto offset = request->offset();
    auto length = cntl->request_attachment().size();
    status = chunk->append(cntl->request_attachment(), request->offset());
    if (!status.ok()) {
        PLOG_ERROR(("desc", "failed to append chunk") //
                   ("uuid", uuid.str())               //
                   ("errno", status.error_code())     //
                   ("error", status.error_str()));
        cntl->SetFailed(status.error_code(), "%s", status.error_cstr());
        return;
    }

    // return new offset
    response->set_offset(chunk->size());
}

void ManusyaServiceImpl::list_chunk(google::protobuf::RpcController* controller,
                                    const pain::core::manusya::ListChunkRequest* request,
                                    pain::core::manusya::ListChunkResponse* response,
                                    google::protobuf::Closure* done) {
    DEFINE_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);

    auto uuid = UUID(request->start().high(), request->start().low());

    PLOG_DEBUG(("desc", __func__)                                                //
               ("remote_side", butil::endpoint2str(cntl->remote_side()).c_str()) //
               ("start", uuid.str())                                             //
               ("limit", request->limit()));

    Bank::instance().list_chunk(uuid, request->limit(), [&](UUID uuid) {
        auto* u = response->add_uuids();
        u->set_low(uuid.low());
        u->set_high(uuid.high());
    });
}

void ManusyaServiceImpl::read_chunk(google::protobuf::RpcController* controller,
                                    const pain::core::manusya::ReadChunkRequest* request,
                                    pain::core::manusya::ReadChunkResponse* response,
                                    google::protobuf::Closure* done) {
    DEFINE_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);
    UUID uuid(request->uuid().high(), request->uuid().low());
    span->SetAttribute("chunk", uuid.str());

    PLOG_DEBUG(("desc", __func__)                                                //
               ("remote_side", butil::endpoint2str(cntl->remote_side()).c_str()) //
               ("chunk", uuid.str())                                             //
               ("offset", request->offset())                                     //
               ("attached", cntl->request_attachment().size()));

    ChunkPtr chunk;
    auto status = Bank::instance().get_chunk(uuid, &chunk);
    if (!status.ok()) {
        PLOG_ERROR(("desc", "chunk not found")("uuid", uuid.str()));
        cntl->SetFailed(ENOENT, "Chunk not found");
        return;
    }

    status = chunk->read(request->offset(), request->length(), &cntl->response_attachment());
    if (!status.ok()) {
        PLOG_ERROR(("desc", "failed to read chunk")("uuid", uuid.str())("error", status.error_str()));
        cntl->SetFailed(status.error_code(), "%s", status.error_cstr());
        return;
    }
}

void ManusyaServiceImpl::seal_chunk(google::protobuf::RpcController* controller,
                                    const pain::core::manusya::SealChunkRequest* request,
                                    pain::core::manusya::SealChunkResponse* response,
                                    google::protobuf::Closure* done) {
    DEFINE_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);
    UUID uuid(request->uuid().high(), request->uuid().low());
    span->SetAttribute("chunk", uuid.str());

    PLOG_DEBUG(("desc", __func__)                                                //
               ("remote_side", butil::endpoint2str(cntl->remote_side()).c_str()) //
               ("chunk", uuid.str()));

    ChunkPtr chunk;
    auto status = Bank::instance().get_chunk(uuid, &chunk);
    if (!status.ok()) {
        PLOG_ERROR(("desc", "chunk not found")("uuid", uuid.str()));
        cntl->SetFailed(ENOENT, "Chunk not found");
        return;
    }

    status = chunk->seal();
    if (!status.ok()) {
        PLOG_ERROR(("desc", "failed to seal chunk")("uuid", uuid.str())("error", status.error_str()));
        cntl->SetFailed(status.error_code(), "%s", status.error_cstr());
        return;
    }
}

void ManusyaServiceImpl::remove_chunk(google::protobuf::RpcController* controller,
                                      const pain::core::manusya::RemoveChunkRequest* request,
                                      pain::core::manusya::RemoveChunkResponse* response,
                                      google::protobuf::Closure* done) {
    DEFINE_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);
    UUID uuid(request->uuid().high(), request->uuid().low());
    span->SetAttribute("chunk", uuid.str());

    PLOG_DEBUG(("desc", __func__)                                                //
               ("remote_side", butil::endpoint2str(cntl->remote_side()).c_str()) //
               ("chunk", uuid.str()));

    auto status = Bank::instance().remove_chunk(uuid);
    if (!status.ok()) {
        PLOG_ERROR(("desc", "failed to remove chunk")("uuid", uuid.str())("error", status.error_str()));
        cntl->SetFailed(status.error_code(), "%s", status.error_cstr());
        return;
    }
}

void ManusyaServiceImpl::query_chunk(google::protobuf::RpcController* controller,
                                     const pain::core::manusya::QueryChunkRequest* request,
                                     pain::core::manusya::QueryChunkResponse* response,
                                     google::protobuf::Closure* done) {
    DEFINE_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);
    UUID uuid(request->uuid().high(), request->uuid().low());
    span->SetAttribute("chunk", uuid.str());

    PLOG_DEBUG(("desc", __func__)                                                //
               ("remote_side", butil::endpoint2str(cntl->remote_side()).c_str()) //
               ("chunk", uuid.str()));

    ChunkPtr chunk;
    auto status = Bank::instance().get_chunk(uuid, &chunk);
    if (!status.ok()) {
        PLOG_ERROR(("desc", "chunk not found")("uuid", uuid.str()));
        cntl->SetFailed(ENOENT, "Chunk not found");
        return;
    }

    response->set_size(chunk->size());
    response->mutable_chunk_options()->set_append_out_of_order(chunk->options().append_out_of_order);
    response->mutable_chunk_options()->set_digest(chunk->options().digest);
    response->set_chunk_state(static_cast<pain::core::ChunkState>(chunk->state()));
}

} // namespace pain::manusya
