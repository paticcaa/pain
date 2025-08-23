#include "deva/deva.h"
#include <pain/base/plog.h>
#include <pain/base/uuid.h>
#include "deva/macro.h"

#define DEVA_METHOD(name)                                                                                              \
    Status Deva::name([[maybe_unused]] const pain::proto::deva::store::name##Request* request,                         \
                      [[maybe_unused]] pain::proto::deva::store::name##Response* response,                             \
                      [[maybe_unused]] int64_t index)

namespace pain::deva {

Status Deva::create(const std::string& path, const UUID& id, FileType type) {
    SPAN(span);
    PLOG_DEBUG(("desc", "create")("path", path)("id", id.str())("type", type));
    // get parent
    std::filesystem::path file_path(path);
    auto dir = file_path.parent_path();
    auto filename = file_path.filename();
    auto file_type = FileType::kFile;
    UUID parent_dir_uuid;
    auto status = _namespace.lookup(dir.c_str(), &parent_dir_uuid, &file_type);
    if (!status.ok()) {
        return status;
    }

    if (file_type != FileType::kDirectory) {
        return Status(EINVAL, fmt::format("{} is not a directory", dir.c_str()));
    }

    UUID file_uuid(id.high(), id.low());
    status = _namespace.create(parent_dir_uuid, filename, type, file_uuid);
    if (!status.ok()) {
        return status;
    }

    return Status::OK();
}

DEVA_METHOD(CreateFile) {
    SPAN(span);
    PLOG_DEBUG(("desc", "create_file")("index", index)("request", request->DebugString()));
    auto& path = request->path();
    auto& file_id = request->file_id();
    UUID file_uuid(file_id.high(), file_id.low());
    auto status = create(path, file_uuid, FileType::kFile);
    if (!status.ok()) {
        return status;
    }
    auto file_info = response->mutable_file_info();
    file_info->mutable_file_id()->set_high(file_uuid.high());
    file_info->mutable_file_id()->set_low(file_uuid.low());
    file_info->set_type(pain::proto::FileType::FILE_TYPE_FILE);
    file_info->set_size(0);
    file_info->set_atime(request->atime());
    file_info->set_mtime(request->mtime());
    file_info->set_ctime(request->ctime());
    file_info->set_mode(request->mode());
    file_info->set_uid(request->uid());
    file_info->set_gid(request->gid());
    _file_infos[file_uuid] = *file_info;
    return Status::OK();
}

DEVA_METHOD(CreateDir) {
    SPAN(span);
    PLOG_DEBUG(("desc", "create_dir")("index", index)("request", request->DebugString()));
    auto& path = request->path();
    auto& dir_id = request->dir_id();
    UUID dir_uuid(dir_id.high(), dir_id.low());
    auto status = create(path, dir_uuid, FileType::kDirectory);
    if (!status.ok()) {
        return status;
    }
    auto file_info = response->mutable_file_info();
    file_info->mutable_file_id()->set_high(dir_uuid.high());
    file_info->mutable_file_id()->set_low(dir_uuid.low());
    file_info->set_type(pain::proto::FileType::FILE_TYPE_DIRECTORY);
    file_info->set_size(0);
    file_info->set_atime(request->atime());
    file_info->set_mtime(request->mtime());
    file_info->set_ctime(request->ctime());
    file_info->set_mode(request->mode());
    file_info->set_uid(request->uid());
    file_info->set_gid(request->gid());
    _file_infos[dir_uuid] = *file_info;
    return Status::OK();
}

DEVA_METHOD(RemoveFile) {
    SPAN(span);
    PLOG_INFO(("desc", "remove_file")("index", index));
    return Status::OK();
}

DEVA_METHOD(SealFile) {
    SPAN(span);
    PLOG_INFO(("desc", "seal_file")("index", index));
    return Status::OK();
}

DEVA_METHOD(CreateChunk) {
    SPAN(span);
    PLOG_INFO(("desc", "remove")("index", index));
    return Status::OK();
}

DEVA_METHOD(CheckInChunk) {
    SPAN(span);
    PLOG_INFO(("desc", "seal")("index", index));
    return Status::OK();
}

DEVA_METHOD(SealChunk) {
    SPAN(span);
    PLOG_INFO(("desc", "create_chunk")("index", index));
    return Status::OK();
}

DEVA_METHOD(SealAndNewChunk) {
    SPAN(span);
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
