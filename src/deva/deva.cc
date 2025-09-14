#include "deva/deva.h"
#include <pain/base/plog.h>
#include <pain/base/uuid.h>
#include "common/object_id_util.h"
#include "common/txn_manager.h"
#include "deva/macro.h"

#define DEVA_METHOD(name)                                                                                              \
    Status Deva::name([[maybe_unused]] int32_t version,                                                                \
                      [[maybe_unused]] const pain::proto::deva::store::name##Request* request,                         \
                      [[maybe_unused]] pain::proto::deva::store::name##Response* response,                             \
                      [[maybe_unused]] int64_t index)

namespace pain::deva {

Status Deva::create(const std::string& path, const ObjectId& id, FileType type) {
    SPAN(span);
    PLOG_DEBUG(("desc", "create")("path", path)("id", id.str())("type", type));
    // get parent
    std::filesystem::path file_path(path);
    auto dir = file_path.parent_path();
    auto filename = file_path.filename();
    auto file_type = FileType::kFile;
    ObjectId parent_dir_uuid;
    auto status = _namespace.lookup(dir.c_str(), &parent_dir_uuid, &file_type);
    if (!status.ok()) {
        return status;
    }

    if (file_type != FileType::kDirectory) {
        return Status(EINVAL, fmt::format("{} is not a directory", dir.c_str()));
    }

    status = _namespace.create(parent_dir_uuid, filename, type, id);
    if (!status.ok()) {
        return status;
    }

    return Status::OK();
}

DEVA_METHOD(CreateFile) {
    SPAN(span);
    PLOG_DEBUG(("desc", "create_file")("version", version)("index", index)("request", request->DebugString()));
    auto& path = request->path();
    auto& file_id = request->file_id();
    ObjectId file_uuid = common::from_proto(file_id);
    auto status = create(path, file_uuid, FileType::kFile);
    if (!status.ok()) {
        return status;
    }
    auto file_info = response->mutable_file_info();
    common::to_proto(file_uuid, file_info->mutable_file_id());
    file_info->set_type(pain::proto::FileType::FILE_TYPE_FILE);
    file_info->set_size(0);
    file_info->set_atime(request->atime());
    file_info->set_mtime(request->mtime());
    file_info->set_ctime(request->ctime());
    file_info->set_mode(request->mode());
    file_info->set_uid(request->uid());
    file_info->set_gid(request->gid());
    status = update_file_info(file_uuid, *file_info);
    if (!status.ok()) {
        return status;
    }
    return Status::OK();
}

DEVA_METHOD(CreateDir) {
    SPAN(span);
    PLOG_DEBUG(("desc", "create_dir")("version", version)("index", index)("request", request->DebugString()));
    auto& path = request->path();
    auto& dir_id = request->dir_id();
    ObjectId dir_uuid = common::from_proto(dir_id);
    auto status = create(path, dir_uuid, FileType::kDirectory);
    if (!status.ok()) {
        return status;
    }
    auto file_info = response->mutable_file_info();
    common::to_proto(dir_uuid, file_info->mutable_file_id());
    file_info->set_type(pain::proto::FileType::FILE_TYPE_DIRECTORY);
    file_info->set_size(0);
    file_info->set_atime(request->atime());
    file_info->set_mtime(request->mtime());
    file_info->set_ctime(request->ctime());
    file_info->set_mode(request->mode());
    file_info->set_uid(request->uid());
    file_info->set_gid(request->gid());
    status = update_file_info(dir_uuid, *file_info);
    if (!status.ok()) {
        return status;
    }
    return Status::OK();
}

DEVA_METHOD(ReadDir) {
    SPAN(span);
    PLOG_DEBUG(("desc", "read_dir")("version", version)("index", index)("request", request->DebugString()));
    auto& path = request->path();
    ObjectId parent_dir_uuid;
    FileType file_type = FileType::kNone;
    auto status = _namespace.lookup(path.c_str(), &parent_dir_uuid, &file_type);
    if (!status.ok()) {
        return status;
    }
    if (file_type != FileType::kDirectory) {
        return Status(EINVAL, fmt::format("{} is not a directory", path.c_str()));
    }
    std::list<DirEntry> entries;
    _namespace.list(parent_dir_uuid, &entries);
    for (auto& entry : entries) {
        auto dir_entry = response->add_entries();
        common::to_proto(entry.inode, dir_entry->mutable_file_id());
        dir_entry->set_type(static_cast<pain::proto::FileType>(entry.type));
        dir_entry->set_name(std::move(entry.name));
    }
    return Status::OK();
}

DEVA_METHOD(RemoveFile) {
    SPAN(span);
    PLOG_INFO(("desc", "remove_file")("version", version)("index", index));
    return Status::OK();
}

DEVA_METHOD(SealFile) {
    SPAN(span);
    PLOG_INFO(("desc", "seal_file")("version", version)("index", index));
    return Status::OK();
}

DEVA_METHOD(CreateChunk) {
    SPAN(span);
    PLOG_INFO(("desc", "remove")("version", version)("index", index));
    return Status::OK();
}

DEVA_METHOD(CheckInChunk) {
    SPAN(span);
    PLOG_INFO(("desc", "seal")("version", version)("index", index));
    return Status::OK();
}

DEVA_METHOD(SealChunk) {
    SPAN(span);
    PLOG_INFO(("desc", "create_chunk")("version", version)("index", index));
    return Status::OK();
}

DEVA_METHOD(SealAndNewChunk) {
    SPAN(span);
    PLOG_INFO(("desc", "remove_chunk")("version", version)("index", index));
    return Status::OK();
}

DEVA_METHOD(GetFileInfo) {
    SPAN(span);
    PLOG_INFO(("desc", "get_file_info")("version", version)("index", index));
    auto& path = request->path();
    ObjectId file_uuid;
    FileType file_type = FileType::kNone;
    auto status = _namespace.lookup(path.c_str(), &file_uuid, &file_type);
    if (!status.ok()) {
        return status;
    }
    if (file_type != FileType::kFile) {
        return Status(EINVAL, fmt::format("{} is not a file", path.c_str()));
    }
    proto::FileInfo file_info;
    status = get_file_info(file_uuid, &file_info);
    if (!status.ok()) {
        return status;
    }
    response->mutable_file_info()->Swap(&file_info);
    return Status::OK();
}

DEVA_METHOD(ManusyaHeartbeat) {
    SPAN(span);
    PLOG_INFO(("desc", "manusya_heartbeat")("version", version)("index", index));
    auto& manusya_id = request->manusya_registration().manusya_id();
    // auto& storage_info = request->manusya_registration().storage_info();
    // TODO: check cluster id

    UUID manusya_uuid(manusya_id.uuid().high(), manusya_id.uuid().low());
    auto it = _manusya_descriptors.find(manusya_uuid);
    if (it == _manusya_descriptors.end()) {
        // new manusya
        PLOG_INFO(("desc", "new manusya")              //
                  ("manusya_uuid", manusya_uuid.str()) //
                  ("ip", manusya_id.ip())              //
                  ("port", manusya_id.port()));
        ManusyaDescriptor manusya_descriptor;
        manusya_descriptor.ip = manusya_id.ip();
        manusya_descriptor.port = manusya_id.port();
        manusya_descriptor.uuid = manusya_uuid;
        manusya_descriptor.is_alive = true;
        manusya_descriptor.update_heartbeat();
        _manusya_descriptors[manusya_uuid] = manusya_descriptor;
    } else {
        auto& manusya_descriptor = it->second;
        manusya_descriptor.ip = manusya_id.ip();
        manusya_descriptor.port = manusya_id.port();
        manusya_descriptor.update_heartbeat();
    }
    return Status::OK();
}

DEVA_METHOD(ListManusya) {
    SPAN(span);
    PLOG_INFO(("desc", "list_manusya")("version", version)("index", index));
    auto manusya_descriptors = response->mutable_manusya_descriptors();
    for (auto& [_, manusya_descriptor] : _manusya_descriptors) {
        auto manusya_descriptor_proto = manusya_descriptors->Add();
        manusya_descriptor_proto->mutable_manusya_id()->set_ip(manusya_descriptor.ip);
        manusya_descriptor_proto->mutable_manusya_id()->set_port(manusya_descriptor.port);
        manusya_descriptor_proto->mutable_manusya_id()->mutable_uuid()->set_high(manusya_descriptor.uuid.high());
        manusya_descriptor_proto->mutable_manusya_id()->mutable_uuid()->set_low(manusya_descriptor.uuid.low());
        manusya_descriptor_proto->mutable_storage_info()->set_cluster_id(manusya_descriptor.cluster_id);
        manusya_descriptor_proto->set_is_alive(manusya_descriptor.is_alive);
        manusya_descriptor_proto->set_last_heartbeat_time(manusya_descriptor.last_heartbeat_time);
    }
    return Status::OK();
}

Status Deva::set_applied_index(int64_t index) {
    if (index <= _applied_index) {
        PLOG_WARN(("desc", "index is applied already")("index", index));
        return Status::OK();
    }
    auto in_txn = common::TxnManager::instance().in_txn();
    auto this_txn = _store->begin_txn();
    auto txn = in_txn ? common::TxnManager::instance().get_txn_store() : this_txn.get();
    if (txn == nullptr) {
        return Status(EIO, "Failed to begin transaction");
    }
    auto status = txn->hset(_meta_key, _applied_index_key, std::to_string(index));
    return status;
    if (in_txn) {
        return Status::OK();
    }

    status = txn->commit();
    return status;
}

Status Deva::update_file_info(const ObjectId& id, const proto::FileInfo& file_info) {
    auto in_txn = common::TxnManager::instance().in_txn();
    auto this_txn = _store->begin_txn();
    auto txn = in_txn ? common::TxnManager::instance().get_txn_store() : this_txn.get();
    if (txn == nullptr) {
        return Status(EIO, "Failed to begin transaction");
    }
    auto status = txn->hset(_file_info_key, id.str(), file_info.SerializeAsString());
    if (!status.ok()) {
        return status;
    }
    if (in_txn) {
        return Status::OK();
    }
    status = txn->commit();
    if (!status.ok()) {
        return status;
    }
    return status;
}

Status Deva::get_file_info(const ObjectId& id, proto::FileInfo* file_info) {
    std::string file_info_str;
    auto status = _store->hget(_file_info_key, id.str(), &file_info_str);
    if (!status.ok()) {
        return status;
    }
    auto ret = file_info->ParseFromString(file_info_str);
    if (!ret) {
        return Status(EIO, "Failed to parse file info");
    }
    return Status::OK();
}

Status Deva::remove_file_info(const ObjectId& id) {
    auto status = _store->hdel(_file_info_key, id.str());
    if (!status.ok()) {
        return status;
    }
    return Status::OK();
}

Status Deva::save_snapshot(std::string_view path, std::vector<std::string>* files) {
    return _store->check_point(path.data(), files);
}

Status Deva::load_snapshot(std::string_view path) {
    auto status = _store->recover(path.data());
    if (!status.ok()) {
        return status;
    }
    // get applied index
    std::string applied_index_str;
    status = _store->hget(_meta_key, _applied_index_key, &applied_index_str);
    if (!status.ok()) {
        return status;
    }
    _applied_index = std::stoll(applied_index_str);
    return Status::OK();
}

} // namespace pain::deva
