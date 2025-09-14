#include "deva/namespace.h"
#include <pain/base/plog.h>
#include <pain/base/scope_exit.h>
#include "common/object_id_util.h"
#include "common/txn_manager.h"
#include "common/txn_store.h"

namespace pain::deva {

Namespace::Namespace(common::StorePtr store) : _store(store) {
    _root = ObjectId::from_str_or_die("00000000-00000000-0000-0000-0000-000000000000");
}

Status Namespace::load() {
    return Status::OK();
}

Status Namespace::create(const ObjectId& parent, const std::string& name, FileType type, const ObjectId& inode) {
    auto in_txn = common::TxnManager::instance().in_txn();
    auto this_txn = _store->begin_txn();
    auto txn = in_txn ? common::TxnManager::instance().get_txn_store() : this_txn.get();
    if (txn == nullptr) {
        return Status(EIO, "Failed to begin transaction");
    }
    auto rollback = make_scope_exit([&txn]() {
        auto status = txn->rollback();
        if (!status.ok()) {
            PLOG_ERROR(("desc", "Failed to rollback")("status", status));
        }
    });
    if (in_txn) {
        rollback.release();
    }
    // find parent
    std::string dentries_str;
    proto::DirEntries dentries;
    auto status = txn->hget(_dentry_key, parent.str(), &dentries_str);
    if (status.ok()) {
        if (!dentries.ParseFromString(dentries_str)) {
            return Status(EBADMSG, "Failed to parse dentries");
        }
    } else if (status.error_code() != ENOENT) {
        // something wrong
        return status;
    }
    // check if name or inode already exists
    for (const auto& dentry : dentries.entries()) {
        if (dentry.name() == name) {
            return Status(EEXIST, "File name already exists");
        }
        if (dentry.file_id() == inode) {
            return Status(EEXIST, "File inode already exists");
        }
    }
    // create dentry
    auto dentry = dentries.add_entries();
    dentry->set_name(name);
    common::to_proto(inode, dentry->mutable_file_id());
    common::to_proto(parent, dentry->mutable_parent_file_id());
    dentry->set_type(static_cast<proto::FileType>(type));
    dentries_str.clear();
    if (!dentries.SerializeToString(&dentries_str)) {
        return Status(EBADMSG, "Failed to serialize dentries");
    }
    // create dentry
    txn->hset(_dentry_key, parent.str(), dentries_str);

    // insert file info
    proto::FileInfo file_info;
    common::to_proto(inode, file_info.mutable_file_id());
    file_info.set_type(static_cast<proto::FileType>(type));
    file_info.set_size(0);
    file_info.set_ctime(0);
    file_info.set_mtime(0);
    file_info.set_atime(0);
    file_info.set_uid(0);
    file_info.set_gid(0);
    std::string file_info_str;
    if (!file_info.SerializeToString(&file_info_str)) {
        return Status(EBADMSG, "Failed to serialize file info");
    }
    status = txn->hset(_inode_key, inode.str(), file_info_str);
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
    rollback.release();
    return Status::OK();
}

Status Namespace::remove(const ObjectId& parent, const std::string& name) {
    auto in_txn = common::TxnManager::instance().in_txn();
    auto this_txn = _store->begin_txn();
    auto txn = in_txn ? common::TxnManager::instance().get_txn_store() : this_txn.get();
    if (txn == nullptr) {
        return Status(EIO, "Failed to begin transaction");
    }
    // find parent
    auto rollback = make_scope_exit([&txn]() {
        auto status = txn->rollback();
        if (!status.ok()) {
            PLOG_ERROR(("desc", "Failed to rollback")("status", status));
        }
    });
    if (in_txn) {
        rollback.release();
    }
    // get dentries
    std::string dentries_str;
    auto status = txn->hget(_dentry_key, parent.str(), &dentries_str);
    if (!status.ok()) {
        return status;
    }
    proto::DirEntries dentries;
    if (!dentries.ParseFromString(dentries_str)) {
        return Status(EBADMSG, "Failed to parse dentries");
    }
    // remove dentry
    ObjectId file_id;
    auto entry =
        std::find_if(dentries.entries().begin(), dentries.entries().end(), [&name](const proto::DirEntry& dentry) {
            return dentry.name() == name;
        });
    if (entry == dentries.entries().end()) {
        return Status(ENOENT, "No such file or directory");
    }
    file_id = common::from_proto(entry->file_id());
    dentries.mutable_entries()->erase(entry);
    // set dentries
    if (!dentries.SerializeToString(&dentries_str)) {
        return Status(EBADMSG, "Failed to serialize dentries");
    }
    status = txn->hset(_dentry_key, parent.str(), dentries_str);
    if (!status.ok()) {
        return status;
    }

    // remove file info
    status = txn->hdel(_inode_key, file_id.str());
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
    rollback.release();
    return Status::OK();
}

void Namespace::list(const ObjectId& parent, std::list<DirEntry>* entries) const {
    entries->clear();
    std::string dentries_str;
    auto in_txn = common::TxnManager::instance().in_txn();
    auto this_txn = _store->begin_txn();
    auto txn = in_txn ? common::TxnManager::instance().get_txn_store() : this_txn.get();
    if (txn == nullptr) {
        return;
    }
    auto status = txn->hget(_dentry_key, parent.str(), &dentries_str);
    if (!status.ok()) {
        PLOG_ERROR(("desc", "Failed to get dentries")("status", status));
        return;
    }
    proto::DirEntries dentries;
    if (!dentries.ParseFromString(dentries_str)) {
        PLOG_ERROR(("desc", "Failed to parse dentries")("status", status));
        return;
    }
    for (const auto& dentry : dentries.entries()) {
        entries->emplace_back(
            common::from_proto(dentry.file_id()), dentry.name(), static_cast<FileType>(dentry.type()));
    }
    if (in_txn) {
        return;
    }

    status = txn->commit();
    if (!status.ok()) {
        PLOG_ERROR(("desc", "Failed to commit")("status", status));
    }
}

// parse path such as /a/b/c to ["a", "b", "c"]
Status Namespace::parse_path(const char* path, std::list<std::string_view>* components) const {
    const char* p = path;
    if (*p != '/') {
        return Status(EINVAL, fmt::format("Invalid path:{}", path));
    }

    while (*p != '\0') {
        if (*p == '/') {
            p++;
            continue;
        }
        const char* q = p;
        while (*q != '\0' && *q != '/') {
            q++;
        }
        components->emplace_back(p, q - p);
        p = q;
    }
    return Status::OK();
}

Status Namespace::lookup(const char* path, ObjectId* inode, FileType* file_type) const {
    std::list<std::string_view> components;
    auto status = parse_path(path, &components);
    if (!status.ok()) {
        return status;
    }

    ObjectId parent = _root;
    *file_type = FileType::kDirectory;

    auto in_txn = common::TxnManager::instance().in_txn();
    auto this_txn = _store->begin_txn();
    auto txn = in_txn ? common::TxnManager::instance().get_txn_store() : this_txn.get();
    if (txn == nullptr) {
        return Status(EIO, "Failed to begin transaction");
    }

    for (const auto& component : components) {
        std::string dentries_str;
        auto status = txn->hget(_dentry_key, parent.str(), &dentries_str);
        if (!status.ok()) {
            return status;
        }
        proto::DirEntries dentries;
        if (!dentries.ParseFromString(dentries_str)) {
            return Status(EBADMSG, fmt::format("Failed to parse dentries: {}", component));
        }
        auto entry = std::find_if(
            dentries.entries().begin(), dentries.entries().end(), [&component](const proto::DirEntry& dentry) {
                return dentry.name() == component;
            });
        if (entry == dentries.entries().end()) {
            return Status(ENOENT, fmt::format("No such file or directory: {}", component));
        }
        if (entry->type() == static_cast<proto::FileType>(FileType::kFile) && component != components.back()) {
            return Status(ENOENT, fmt::format("Not a directory: {}", component));
        }
        parent = common::from_proto(entry->file_id());
        *file_type = static_cast<FileType>(entry->type());
    }
    *inode = parent;
    if (in_txn) {
        return Status::OK();
    }

    status = txn->commit();
    if (!status.ok()) {
        PLOG_ERROR(("desc", "Failed to commit")("status", status));
    }
    return status;
}

} // namespace pain::deva
