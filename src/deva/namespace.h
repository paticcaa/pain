#pragma once

#include <bthread/mutex.h>
#include <list>
#include <map>
#include <fmt/format.h>

#include <pain/base/object_id.h>
#include <pain/base/types.h>
#include "pain/proto/common.pb.h"
#include "common/store.h"

namespace pain::deva {

enum class FileType {
    kNone = 0,
    kFile = 1,
    kDirectory = 2,
};

struct DirEntry {
    ObjectId inode;
    std::string name;
    FileType type;
};

class Namespace {
public:
    Namespace(common::StorePtr store);
    Status load();
    const ObjectId& root() const {
        return _root;
    }
    Status create(const ObjectId& parent, const std::string& name, FileType type, const ObjectId& inode);
    Status remove(const ObjectId& parent, const std::string& name);
    void list(const ObjectId& parent, std::list<DirEntry>* entries) const;
    Status lookup(const char* path, ObjectId* inode, FileType* file_type) const;

private:
    Status parse_path(const char* path, std::list<std::string_view>* components) const;
    ObjectId _root;
    common::StorePtr _store;
    const char* _dentry_key = "dentry";
    const char* _inode_key = "inode";
};

} // namespace pain::deva

template <>
struct fmt::formatter<pain::deva::FileType> : fmt::formatter<std::string_view> {
    template <typename FormatContext>
    auto format(pain::deva::FileType t, FormatContext& ctx) const {
        std::string_view name;
        switch (t) {
        case pain::deva::FileType::kNone:
            name = "NONE";
            break;
        case pain::deva::FileType::kFile:
            name = "FILE";
            break;
        case pain::deva::FileType::kDirectory:
            name = "DIRECTORY";
            break;
        }
        return fmt::formatter<std::string_view>::format(name, ctx);
    }
};

template <>
struct fmt::formatter<pain::deva::DirEntry> : fmt::formatter<std::string_view> {
    template <typename FormatContext>
    auto format(const pain::deva::DirEntry& entry, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{{inode:{}, name:{}, type:{}}}", entry.inode.str(), entry.name, entry.type);
    }
};
