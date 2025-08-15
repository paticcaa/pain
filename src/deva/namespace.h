#pragma once

#include <bthread/mutex.h>
#include <list>
#include <map>
#include <fmt/format.h>

#include "base/types.h"
#include "base/uuid.h"

namespace pain::deva {

enum class FileType {
    kFile = 0,
    kDirectory,
};

struct DirEntry {
    UUID inode;
    std::string name;
    FileType type;
};

class Namespace {
public:
    static Namespace& instance();
    Status load();
    const UUID& root() const {
        return _root;
    }
    Status create(const UUID& parent, const std::string& name, FileType type, const UUID& inode);
    Status remove(const UUID& parent, const std::string& name);
    void list(const UUID& parent, std::list<DirEntry>* entries) const;
    Status lookup(const char* path, UUID* inode, FileType* file_type) const;

private:
    Status parse_path(const char* path, std::list<std::string_view>* components) const;
    Namespace();
    UUID _root;
    std::map<UUID, std::list<DirEntry>> _entries;
    mutable bthread::Mutex _mutex;
};

} // namespace pain::deva

template <>
struct fmt::formatter<pain::deva::FileType> : fmt::formatter<std::string_view> {
    template <typename FormatContext>
    auto format(pain::deva::FileType t, FormatContext& ctx) const {
        std::string_view name;
        switch (t) {
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
