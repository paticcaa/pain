#include "manusya/manusya_service_impl.h"

#include <brpc/controller.h>

#include <pain/base/plog.h>
#include <pain/base/tracer.h>
#include "butil/endpoint.h"
#include "common/object_id_util.h"
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
    auto status = Bank::instance().create_chunk(options, request->partition_id(), &chunk);
    if (!status.ok()) {
        PLOG_ERROR(("desc", "failed to create chunk")("error", status.error_str()));
        response->mutable_header()->set_status(status.error_code());
        response->mutable_header()->set_message(status.error_str());
        return;
    }

    common::to_proto(chunk->chunk_id(), response->mutable_chunk_id());
}

MANUSYA_SERVICE_METHOD(AppendChunk) {
    DEFINE_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);
    ObjectId chunk_id = common::from_proto(request->chunk_id());
    span->SetAttribute("chunk", chunk_id.str());
    PLOG_DEBUG(("desc", __func__)                                                //
               ("remote_side", butil::endpoint2str(cntl->remote_side()).c_str()) //
               ("chunk", chunk_id.str())                                         //
               ("offset", request->offset())                                     //
               ("attached", cntl->request_attachment().size()));

    ChunkPtr chunk;
    auto status = Bank::instance().get_chunk(chunk_id, &chunk);
    if (!status.ok()) {
        PLOG_ERROR(("desc", "chunk not found")("chunk", chunk_id.str()));
        response->mutable_header()->set_status(ENOENT);
        response->mutable_header()->set_message("Chunk not found");
        return;
    }

    status = chunk->append(cntl->request_attachment(), request->offset());
    if (!status.ok()) {
        PLOG_ERROR(("desc", "failed to append chunk") //
                   ("chunk", chunk_id.str())          //
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

    auto object_id = common::from_proto(request->start());

    PLOG_DEBUG(("desc", __func__)                                                //
               ("remote_side", butil::endpoint2str(cntl->remote_side()).c_str()) //
               ("start", object_id.str())                                        //
               ("limit", request->limit()));

    Bank::instance().list_chunk(object_id, request->limit(), [&](ObjectId object_id) {
        auto* u = response->add_chunk_ids();
        common::to_proto(object_id, u);
    });
}

MANUSYA_SERVICE_METHOD(ReadChunk) {
    DEFINE_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);
    auto object_id = common::from_proto(request->chunk_id());
    span->SetAttribute("chunk", object_id.str());

    PLOG_DEBUG(("desc", __func__)                                                //
               ("remote_side", butil::endpoint2str(cntl->remote_side()).c_str()) //
               ("chunk", object_id.str())                                        //
               ("offset", request->offset())                                     //
               ("attached", cntl->request_attachment().size()));

    ChunkPtr chunk;
    auto status = Bank::instance().get_chunk(object_id, &chunk);
    if (!status.ok()) {
        PLOG_ERROR(("desc", "chunk not found")("chunk", object_id.str()));
        response->mutable_header()->set_status(ENOENT);
        response->mutable_header()->set_message("Chunk not found");
        return;
    }

    status = chunk->read(request->offset(), request->length(), &cntl->response_attachment());
    if (!status.ok()) {
        PLOG_ERROR(("desc", "failed to read chunk")("chunk", object_id.str())("error", status.error_str()));
        cntl->SetFailed(status.error_code(), "%s", status.error_cstr());
        return;
    }
}

MANUSYA_SERVICE_METHOD(QueryAndSealChunk) {
    DEFINE_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);
    auto object_id = common::from_proto(request->chunk_id());
    span->SetAttribute("chunk", object_id.str());

    PLOG_DEBUG(("desc", __func__)                                                //
               ("remote_side", butil::endpoint2str(cntl->remote_side()).c_str()) //
               ("chunk", object_id.str()));

    ChunkPtr chunk;
    auto status = Bank::instance().get_chunk(object_id, &chunk);
    if (!status.ok()) {
        PLOG_ERROR(("desc", "chunk not found")("chunk", object_id.str()));
        cntl->SetFailed(ENOENT, "Chunk not found");
        return;
    }

    uint64_t size = 0;
    status = chunk->query_and_seal(&size);
    if (!status.ok()) {
        PLOG_ERROR(("desc", "failed to seal chunk")("chunk", object_id.str())("error", status.error_str()));
        response->mutable_header()->set_status(status.error_code());
        response->mutable_header()->set_message(status.error_str());
        return;
    }

    response->set_size(size);
}

MANUSYA_SERVICE_METHOD(RemoveChunk) {
    DEFINE_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);
    auto object_id = common::from_proto(request->chunk_id());
    span->SetAttribute("chunk", object_id.str());

    PLOG_DEBUG(("desc", __func__)                                                //
               ("remote_side", butil::endpoint2str(cntl->remote_side()).c_str()) //
               ("chunk", object_id.str()));

    auto status = Bank::instance().remove_chunk(object_id);
    if (!status.ok()) {
        PLOG_ERROR(("desc", "failed to remove chunk")("chunk", object_id.str())("error", status.error_str()));
        response->mutable_header()->set_status(status.error_code());
        response->mutable_header()->set_message(status.error_str());
        return;
    }
}

MANUSYA_SERVICE_METHOD(QueryChunk) {
    DEFINE_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);
    auto object_id = common::from_proto(request->chunk_id());
    span->SetAttribute("chunk", object_id.str());

    PLOG_DEBUG(("desc", __func__)                                                //
               ("remote_side", butil::endpoint2str(cntl->remote_side()).c_str()) //
               ("chunk", object_id.str()));

    ChunkPtr chunk;
    auto status = Bank::instance().get_chunk(object_id, &chunk);
    if (!status.ok()) {
        PLOG_ERROR(("desc", "chunk not found")("chunk", object_id.str()));
        response->mutable_header()->set_status(ENOENT);
        response->mutable_header()->set_message("Chunk not found");
        return;
    }

    response->set_size(chunk->size());
    response->set_sealed(chunk->state() == ChunkState::kSealed);
}

} // namespace pain::manusya
