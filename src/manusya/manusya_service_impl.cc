#include "manusya/manusya_service_impl.h"

#include <brpc/controller.h>

#include "base/plog.h"
#include "base/tracer.h"
#include "butil/endpoint.h"
#include "manusya/macro.h"

DEFINE_string(manusya_store, "memory", "The path to store the data of manusya");

namespace pain::manusya {

ManusyaServiceImpl::ManusyaServiceImpl() {
    _store = Store::create(FLAGS_manusya_store.c_str());
}

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
    auto status = Chunk::create(options, _store, &chunk);
    if (!status.ok()) {
        PLOG_ERROR(("desc", "failed to create chunk")("error", status.error_str()));
        cntl->SetFailed(status.error_code(), "%s", status.error_cstr());
        return;
    }

    // BUGS: not thread safe
    _chunks[chunk->uuid()] = chunk;

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

    auto it = _chunks.find(uuid);
    if (it == _chunks.end()) {
        PLOG_ERROR(("desc", "chunk not found")("uuid", uuid.str()));
        cntl->SetFailed(ENOENT, "Chunk not found");
        return;
    }

    auto offset = request->offset();
    auto length = cntl->request_attachment().size();
    auto chunk = it->second;
    auto status = chunk->append(cntl->request_attachment(), request->offset());
    if (!status.ok()) {
        PLOG_ERROR(("desc", "failed to append chunk")("uuid", uuid.str())("error", status.error_str()));
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

    PLOG_DEBUG(("desc", __func__)                                                //
               ("remote_side", butil::endpoint2str(cntl->remote_side()).c_str()) //
               ("attached", cntl->request_attachment().size()));

    for (const auto& [uuid, chunk] : _chunks) {
        auto c = response->add_uuids();
        c->set_low(uuid.low());
        c->set_high(uuid.high());
    }
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

    auto it = _chunks.find(uuid);
    if (it == _chunks.end()) {
        PLOG_ERROR(("desc", "chunk not found")("uuid", uuid.str()));
        cntl->SetFailed(ENOENT, "Chunk not found");
        return;
    }

    auto chunk = it->second;
    auto status = chunk->read(request->offset(), request->length(), &cntl->response_attachment());
    if (!status.ok()) {
        PLOG_ERROR(("desc", "failed to read chunk")("uuid", uuid.str())("error", status.error_str()));
        cntl->SetFailed(status.error_code(), "%s", status.error_cstr());
        return;
    }
}

} // namespace pain::manusya
