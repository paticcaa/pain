#include "deva/deva.h"
#include "base/plog.h"
#include "base/uuid.h"
#include "deva/macro.h"

#define DEVA_METHOD(name)                                                                                              \
    Status Deva::name([[maybe_unused]] const pain::proto::deva::store::name##Request* request,                         \
                      [[maybe_unused]] pain::proto::deva::store::name##Response* response,                             \
                      [[maybe_unused]] int64_t index)

namespace pain::deva {

DEVA_METHOD(CreateFile) {
    PLOG_INFO(("desc", "create_file")("index", index));
    return Status::OK();
}

DEVA_METHOD(RemoveFile) {
    PLOG_INFO(("desc", "remove_file")("index", index));
    return Status::OK();
}

DEVA_METHOD(SealFile) {
    PLOG_INFO(("desc", "seal_file")("index", index));
    return Status::OK();
}

DEVA_METHOD(CreateChunk) {
    PLOG_INFO(("desc", "remove")("index", index));
    return Status::OK();
}

DEVA_METHOD(CheckInChunk) {
    PLOG_INFO(("desc", "seal")("index", index));
    return Status::OK();
}

DEVA_METHOD(SealChunk) {
    PLOG_INFO(("desc", "create_chunk")("index", index));
    return Status::OK();
}

DEVA_METHOD(SealAndNewChunk) {
    PLOG_INFO(("desc", "remove_chunk")("index", index));
    return Status::OK();
}

Status Deva::save_snapshot(std::string_view path, std::vector<std::string>* files) {
    std::ignore = path;
    std::ignore = files;
    return Status::OK();
}

Status Deva::load_snapshot(std::string_view path) {
    std::ignore = path;
    return Status::OK();
}

} // namespace pain::deva
