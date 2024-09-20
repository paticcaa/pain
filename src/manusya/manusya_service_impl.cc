#include "manusya/manusya_service_impl.h"
#include <brpc/controller.h>
#include "base/brpc_text_map_carrier.h"
#include "base/plog.h"
#include "base/tracer.h"
#include "butil/endpoint.h"

DEFINE_string(manusya_data_path, "./data", "The path to store the data of manusya");

namespace pain::manusya {

void ManusyaServiceImpl::create_chunk(
    google::protobuf::RpcController *controller,
    const CreateChunkRequest *request,
    CreateChunkResponse *response,
    google::protobuf::Closure *done) {
    DEFINE_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);

    PLOG_INFO(("desc", "Received request")                                      //
              ("log_id", cntl->log_id())                                        //
              ("remote_side", butil::endpoint2str(cntl->remote_side()).c_str()) //
              ("attached", cntl->request_attachment().size()));

    ChunkPtr chunk = Chunk::create();

    // BUGS: not thread safe
    _chunks[chunk->uuid()] = chunk;

    PLOG_INFO(("desc", "Created chunk")("uuid", chunk->uuid().str()));
    response->mutable_uuid()->set_low(chunk->uuid().low());
    response->mutable_uuid()->set_high(chunk->uuid().high());
}

void ManusyaServiceImpl::append_chunk(
    google::protobuf::RpcController *controller,
    const AppendChunkRequest *request,
    AppendChunkResponse *response,
    google::protobuf::Closure *done) {
    DEFINE_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);

    PLOG_INFO(("desc", "Received request")                                      //
              ("log_id", cntl->log_id())                                        //
              ("remote_side", butil::endpoint2str(cntl->remote_side()).c_str()) //
              ("offset", request->offset())                                     //
              ("attached", cntl->request_attachment().size()));

    base::UUID uuid(request->uuid().high(), request->uuid().low());
    auto it = _chunks.find(uuid);
    if (it == _chunks.end()) {
        PLOG_ERROR(("desc", "Chunk not found")("uuid", uuid.str()));
        cntl->SetFailed(ENOENT, "Chunk not found");
        return;
    }

    auto chunk = it->second;
    auto status = chunk->append(cntl->request_attachment(), request->offset());
    if (!status.ok()) {
        PLOG_ERROR(("desc", "Failed to append chunk")("uuid", uuid.str())(
            "error", status.error_str()));
        cntl->SetFailed(status.error_code(), "%s", status.error_cstr());
        return;
    }

    PLOG_INFO(
        ("desc", "Appended chunk")("uuid", uuid.str())("size", chunk->size()));
}

void ManusyaServiceImpl::list_chunk(google::protobuf::RpcController *controller,
                                    const ListChunkRequest *request,
                                    ListChunkResponse *response,
                                    google::protobuf::Closure *done) {
    DEFINE_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);

    PLOG_INFO(("desc", "Received request")                                      //
              ("log_id", cntl->log_id())                                        //
              ("remote_side", butil::endpoint2str(cntl->remote_side()).c_str()) //
              ("attached", cntl->request_attachment().size()));

    for (const auto &[uuid, chunk] : _chunks) {
        auto c = response->add_uuids();
        c->set_low(uuid.low());
        c->set_high(uuid.high());
    }
}

void ManusyaServiceImpl::read_chunk(google::protobuf::RpcController *controller,
                                    const ReadChunkRequest *request,
                                    ReadChunkResponse *response,
                                    google::protobuf::Closure *done) {
    DEFINE_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);

    PLOG_INFO(("desc", "Received request")                                      //
              ("log_id", cntl->log_id())                                        //
              ("remote_side", butil::endpoint2str(cntl->remote_side()).c_str()) //
              ("attached", cntl->request_attachment().size()));

    base::UUID uuid(request->uuid().high(), request->uuid().low());
    auto it = _chunks.find(uuid);
    if (it == _chunks.end()) {
        PLOG_ERROR(("desc", "Chunk not found")("uuid", uuid.str()));
        cntl->SetFailed(ENOENT, "Chunk not found");
        return;
    }

    auto chunk = it->second;
    auto status = chunk->read(request->offset(), request->length(), &cntl->response_attachment());
    if (!status.ok()) {
        PLOG_ERROR(("desc", "Failed to read chunk")("uuid", uuid.str())(
            "error", status.error_str()));
        cntl->SetFailed(status.error_code(), "%s", status.error_cstr());
        return;
    }

    PLOG_INFO(("desc", "Read chunk")("uuid", uuid.str()));
}

} // namespace pain::manusya