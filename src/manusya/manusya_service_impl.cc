#include "manusya/manusya_service_impl.h"

#include <brpc/controller.h>

#include <pain/base/plog.h>
#include <pain/base/tracer.h>
#include "butil/endpoint.h"
#include "manusya/bank.h"
#include "manusya/chunk.h"
#include "manusya/macro.h"

#define MANUSYA_SERVICE_METHOD(name)                                                                                   \
    void ManusyaServiceImpl::name(::google::protobuf::RpcController* controller,                                       \
                                  [[maybe_unused]] const pain::proto::manusya::name##Request* request,                 \
                                  [[maybe_unused]] pain::proto::manusya::name##Response* response,                     \
                                  ::google::protobuf::Closure* done)

namespace pain::manusya {

ManusyaServiceImpl::ManusyaServiceImpl() {}

MANUSYA_SERVICE_METHOD(CreateChunk) {
    DEFINE_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);

    PLOG_DEBUG(("desc", __func__)                                                //
               ("remote_side", butil::endpoint2str(cntl->remote_side()).c_str()) //
               ("attached", cntl->request_attachment().size()));

    ChunkOptions options;

    ChunkPtr chunk;
    auto status = Bank::instance().create_chunk(options, &chunk);
    if (!status.ok()) {
        PLOG_ERROR(("desc", "failed to create chunk")("error", status.error_str()));
        response->mutable_header()->set_status(status.error_code());
        response->mutable_header()->set_message(status.error_str());
        return;
    }

    response->mutable_chunk_id()->set_low(chunk->uuid().low());
    response->mutable_chunk_id()->set_high(chunk->uuid().high());
}

MANUSYA_SERVICE_METHOD(AppendChunk) {
    DEFINE_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);
    UUID uuid(request->chunk_id().high(), request->chunk_id().low());
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
        response->mutable_header()->set_status(ENOENT);
        response->mutable_header()->set_message("Chunk not found");
        return;
    }

    status = chunk->append(cntl->request_attachment(), request->offset());
    if (!status.ok()) {
        PLOG_ERROR(("desc", "failed to append chunk") //
                   ("uuid", uuid.str())               //
                   ("errno", status.error_code())     //
                   ("error", status.error_str()));
        response->mutable_header()->set_status(status.error_code());
        response->mutable_header()->set_message(status.error_str());
        return;
    }

    // return new offset
    response->set_offset(chunk->size());
}

MANUSYA_SERVICE_METHOD(ListChunk) {
    DEFINE_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);

    auto uuid = UUID(request->start().high(), request->start().low());

    PLOG_DEBUG(("desc", __func__)                                                //
               ("remote_side", butil::endpoint2str(cntl->remote_side()).c_str()) //
               ("start", uuid.str())                                             //
               ("limit", request->limit()));

    Bank::instance().list_chunk(uuid, request->limit(), [&](UUID uuid) {
        auto* u = response->add_chunk_ids();
        u->set_low(uuid.low());
        u->set_high(uuid.high());
    });
}

MANUSYA_SERVICE_METHOD(ReadChunk) {
    DEFINE_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);
    UUID uuid(request->chunk_id().high(), request->chunk_id().low());
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
        response->mutable_header()->set_status(ENOENT);
        response->mutable_header()->set_message("Chunk not found");
        return;
    }

    status = chunk->read(request->offset(), request->length(), &cntl->response_attachment());
    if (!status.ok()) {
        PLOG_ERROR(("desc", "failed to read chunk")("uuid", uuid.str())("error", status.error_str()));
        cntl->SetFailed(status.error_code(), "%s", status.error_cstr());
        return;
    }
}

MANUSYA_SERVICE_METHOD(QueryAndSealChunk) {
    DEFINE_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);
    UUID uuid(request->chunk_id().high(), request->chunk_id().low());
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

    uint64_t size = 0;
    status = chunk->query_and_seal(&size);
    if (!status.ok()) {
        PLOG_ERROR(("desc", "failed to seal chunk")("uuid", uuid.str())("error", status.error_str()));
        response->mutable_header()->set_status(status.error_code());
        response->mutable_header()->set_message(status.error_str());
        return;
    }

    response->set_size(size);
}

MANUSYA_SERVICE_METHOD(RemoveChunk) {
    DEFINE_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);
    UUID uuid(request->chunk_id().high(), request->chunk_id().low());
    span->SetAttribute("chunk", uuid.str());

    PLOG_DEBUG(("desc", __func__)                                                //
               ("remote_side", butil::endpoint2str(cntl->remote_side()).c_str()) //
               ("chunk", uuid.str()));

    auto status = Bank::instance().remove_chunk(uuid);
    if (!status.ok()) {
        PLOG_ERROR(("desc", "failed to remove chunk")("uuid", uuid.str())("error", status.error_str()));
        response->mutable_header()->set_status(status.error_code());
        response->mutable_header()->set_message(status.error_str());
        return;
    }
}

MANUSYA_SERVICE_METHOD(QueryChunk) {
    DEFINE_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);
    UUID uuid(request->chunk_id().high(), request->chunk_id().low());
    span->SetAttribute("chunk", uuid.str());

    PLOG_DEBUG(("desc", __func__)                                                //
               ("remote_side", butil::endpoint2str(cntl->remote_side()).c_str()) //
               ("chunk", uuid.str()));

    ChunkPtr chunk;
    auto status = Bank::instance().get_chunk(uuid, &chunk);
    if (!status.ok()) {
        PLOG_ERROR(("desc", "chunk not found")("uuid", uuid.str()));
        response->mutable_header()->set_status(ENOENT);
        response->mutable_header()->set_message("Chunk not found");
        return;
    }

    response->set_size(chunk->size());
    response->set_sealed(chunk->state() == ChunkState::kSealed);
}

} // namespace pain::manusya
