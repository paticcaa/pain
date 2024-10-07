#include "deva/deva.h"
#include "base/plog.h"
#include "base/uuid.h"
#include "deva/macro.h"

namespace pain::deva {

Status Deva::Open(const pain::core::deva::store::OpenRequest* request,
                  pain::core::deva::store::OpenResponse* response,
                  int64_t index) {
    SPAN(span);
    auto uuid = UUID(request->uuid().high(), request->uuid().low());
    PLOG_INFO(("desc", "open")("index", index)("uuid", uuid.str()));

    auto meta = response->mutable_file_meta();
    meta->mutable_inode()->CopyFrom(request->uuid());
    return Status::OK();
}

Status Deva::Close(const pain::core::deva::store::CloseRequest* request,
                   pain::core::deva::store::CloseResponse* response,
                   int64_t index) {
    PLOG_INFO(("desc", "close")("index", index));
    return Status::OK();
}

Status Deva::Remove(const pain::core::deva::store::RemoveRequest* request,
                    pain::core::deva::store::RemoveResponse* response,
                    int64_t index) {
    PLOG_INFO(("desc", "remove")("index", index));
    return Status::OK();
}

Status Deva::Seal(const pain::core::deva::store::SealRequest* request,
                  pain::core::deva::store::SealResponse* response,
                  int64_t index) {
    PLOG_INFO(("desc", "seal")("index", index));
    return Status::OK();
}

Status Deva::CreateChunk(const pain::core::deva::store::CreateChunkRequest* request,
                         pain::core::deva::store::CreateChunkResponse* response,
                         int64_t index) {
    PLOG_INFO(("desc", "create_chunk")("index", index));
    return Status::OK();
}

Status Deva::RemoveChunk(const pain::core::deva::store::RemoveChunkRequest* request,
                         pain::core::deva::store::RemoveChunkResponse* response,
                         int64_t index) {
    PLOG_INFO(("desc", "remove_chunk")("index", index));
    return Status::OK();
}

Status Deva::SealChunk(const pain::core::deva::store::SealChunkRequest* request,
                       pain::core::deva::store::SealChunkResponse* response,
                       int64_t index) {
    PLOG_INFO(("desc", "seal_chunk")("index", index));
    return Status::OK();
}

Status Deva::SealAndNewChunk(const pain::core::deva::store::SealAndNewChunkRequest* request,
                             pain::core::deva::store::SealAndNewChunkResponse* response,
                             int64_t index) {
    PLOG_INFO(("desc", "seal_and_new_chunk")("index", index));
    return Status::OK();
}

Status Deva::save_snapshot(std::string_view path, std::vector<std::string>* files) {
    return Status::OK();
}

Status Deva::load_snapshot(std::string_view path) {
    return Status::OK();
}

} // namespace pain::deva
