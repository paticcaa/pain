#pragma once

#include <bthread/mutex.h>
#include <list>
#include <map>

#include "base/types.h"
#include "base/uuid.h"

namespace pain::deva {

enum class FileType {
    FILE = 0,
    DIRECTORY,
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
    void create(const UUID& parent, const std::string& name, FileType type, const UUID& inode);
    void remove(const UUID& parent, const std::string& name);
    void list(const UUID& parent, std::list<DirEntry>* entries) const;
    Status lookup(const char* path, UUID* inode) const;

private:
    Status parse_path(const char* path, std::list<std::string_view>* components) const;
    Namespace();
    UUID _root;
    std::map<UUID, std::list<DirEntry>> _entries;
    mutable bthread::Mutex _mutex;
};

} // namespace pain::deva
