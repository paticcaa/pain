#include <gtest/gtest.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <fmt/std.h>

#include "deva/namespace.h"

using namespace pain;
using namespace pain::deva;

TEST(Namespace, create) {
    Namespace ns;
    UUID a = UUID::from_str_or_die("00000000-0000-0000-0000-000000000001");
    UUID b = UUID::from_str_or_die("00000000-0000-0000-0000-000000000002");
    UUID c = UUID::from_str_or_die("00000000-0000-0000-0000-000000000003");
    UUID d = UUID::from_str_or_die("00000000-0000-0000-0000-000000000004");
    UUID e = UUID::from_str_or_die("00000000-0000-0000-0000-000000000005");
    UUID f = UUID::from_str_or_die("00000000-0000-0000-0000-000000000006");
    auto status = ns.create(ns.root(), "a", FileType::DIRECTORY, a);
    ASSERT_TRUE(status.ok()) << status.error_str();
    status = ns.create(ns.root(), "b", FileType::DIRECTORY, b);
    ASSERT_TRUE(status.ok()) << status.error_str();
    status = ns.create(ns.root(), "c", FileType::DIRECTORY, c);
    ASSERT_TRUE(status.ok()) << status.error_str();

    std::list<DirEntry> entries;
    ns.list(ns.root(), &entries);
    ASSERT_EQ(entries.size(), 3);

    ns.create(a, "d", FileType::DIRECTORY, d);
    ns.create(a, "e", FileType::DIRECTORY, e);
    ns.create(a, "f", FileType::DIRECTORY, f);

    ns.list(a, &entries);
    ASSERT_EQ(entries.size(), 3);

    ns.remove(ns.root(), "a");
    ns.list(ns.root(), &entries);
    ASSERT_EQ(entries.size(), 2);

    ns.remove(ns.root(), "b");
    ns.list(ns.root(), &entries);
    ASSERT_EQ(entries.size(), 1);

    ns.remove(ns.root(), "c");
    ns.list(ns.root(), &entries);
    ASSERT_EQ(entries.size(), 0);
}

TEST(Namespace, parse_path) {
    Namespace ns;
    std::list<std::string_view> components;
    auto status = ns.parse_path("/a/b/c", &components);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(components.size(), 3);
    ASSERT_EQ(components.front(), "a");
    components.pop_front();
    ASSERT_EQ(components.front(), "b");
    components.pop_front();
    ASSERT_EQ(components.front(), "c");
    components.pop_front();

    status = ns.parse_path("a/b/c", &components);
    ASSERT_FALSE(status.ok());
    status = ns.parse_path("/a/b/c/", &components);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(components.size(), 3);
    ASSERT_EQ(components.front(), "a");
    components.pop_front();
    ASSERT_EQ(components.front(), "b");
    components.pop_front();
    ASSERT_EQ(components.front(), "c");
    components.pop_front();

    status = ns.parse_path("/a/b/c//", &components);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(components.size(), 3);
    ASSERT_EQ(components.front(), "a");
    components.pop_front();
    ASSERT_EQ(components.front(), "b");
    components.pop_front();
    ASSERT_EQ(components.front(), "c");
    components.pop_front();

    status = ns.parse_path("/a/b/c//d", &components);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(components.size(), 4);
    ASSERT_EQ(components.front(), "a");
    components.pop_front();
    ASSERT_EQ(components.front(), "b");
    components.pop_front();
    ASSERT_EQ(components.front(), "c");
    components.pop_front();
    ASSERT_EQ(components.front(), "d");
    components.pop_front();

    status = ns.parse_path("/a/b/c//d/", &components);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(components.size(), 4);
    ASSERT_EQ(components.front(), "a");
    components.pop_front();
    ASSERT_EQ(components.front(), "b");
    components.pop_front();
    ASSERT_EQ(components.front(), "c");
    components.pop_front();
    ASSERT_EQ(components.front(), "d");
    components.pop_front();

    status = ns.parse_path("/a/bc//de/fgh", &components);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(components.size(), 4);
    ASSERT_EQ(components.front(), "a");
    components.pop_front();
    ASSERT_EQ(components.front(), "bc");
    components.pop_front();
    ASSERT_EQ(components.front(), "de");
    components.pop_front();
    ASSERT_EQ(components.front(), "fgh");
    components.pop_front();
}

TEST(Namespace, lookup) {
    UUID a = UUID::from_str_or_die("00000000-0000-0000-0000-000000000001");
    UUID b = UUID::from_str_or_die("00000000-0000-0000-0000-000000000002");
    UUID c = UUID::from_str_or_die("00000000-0000-0000-0000-000000000003");
    UUID d = UUID::from_str_or_die("00000000-0000-0000-0000-000000000004");
    UUID e = UUID::from_str_or_die("00000000-0000-0000-0000-000000000005");
    UUID f = UUID::from_str_or_die("00000000-0000-0000-0000-000000000006");

    Namespace ns;
    ns.create(ns.root(), "a", FileType::DIRECTORY, a);
    ns.create(ns.root(), "b", FileType::DIRECTORY, b);
    ns.create(ns.root(), "c", FileType::DIRECTORY, c);
    ns.create(a, "d", FileType::DIRECTORY, d);
    ns.create(a, "e", FileType::DIRECTORY, e);
    ns.create(a, "f", FileType::FILE, f);

    UUID inode;
    FileType file_type;
    auto status = ns.lookup("/", &inode, &file_type);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(inode, ns.root());
    ASSERT_EQ(file_type, FileType::DIRECTORY);

    status = ns.lookup("/a", &inode, &file_type);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(inode, a);
    ASSERT_EQ(file_type, FileType::DIRECTORY);

    status = ns.lookup("/b", &inode, &file_type);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(inode, b);
    ASSERT_EQ(file_type, FileType::DIRECTORY);

    status = ns.lookup("/c", &inode, &file_type);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(inode, c);
    ASSERT_EQ(file_type, FileType::DIRECTORY);

    status = ns.lookup("/a/d", &inode, &file_type);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(inode, d);
    ASSERT_EQ(file_type, FileType::DIRECTORY);

    status = ns.lookup("/a/e", &inode, &file_type);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(inode, e);
    ASSERT_EQ(file_type, FileType::DIRECTORY);

    status = ns.lookup("/a/f", &inode, &file_type);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(inode, f);
    ASSERT_EQ(file_type, FileType::FILE);

    status = ns.lookup("/a/g", &inode, &file_type);
    ASSERT_FALSE(status.ok());

    status = ns.lookup("/a/f/b", &inode, &file_type);
    ASSERT_FALSE(status.ok());
}

TEST(Namespace, lookup_and_list) {
    Namespace ns;
    UUID a = UUID::from_str_or_die("00000000-0000-0000-0000-000000000001");
    UUID b = UUID::from_str_or_die("00000000-0000-0000-0000-000000000002");
    UUID c = UUID::from_str_or_die("00000000-0000-0000-0000-000000000003");
    UUID d = UUID::from_str_or_die("00000000-0000-0000-0000-000000000004");
    UUID e = UUID::from_str_or_die("00000000-0000-0000-0000-000000000005");
    UUID f = UUID::from_str_or_die("00000000-0000-0000-0000-000000000006");
    auto status = ns.create(ns.root(), "a", FileType::DIRECTORY, a);
    ASSERT_TRUE(status.ok()) << status.error_str();
    status = ns.create(ns.root(), "b", FileType::DIRECTORY, b);
    ASSERT_TRUE(status.ok()) << status.error_str();
    status = ns.create(ns.root(), "c", FileType::DIRECTORY, c);
    ASSERT_TRUE(status.ok()) << status.error_str();

    std::list<DirEntry> entries;
    ns.list(ns.root(), &entries);
    ASSERT_EQ(entries.size(), 3) << fmt::format("{}", fmt::join(entries, ", "));

    ns.create(a, "d", FileType::FILE, d);
    ns.create(a, "e", FileType::FILE, e);
    ns.create(a, "f", FileType::FILE, f);

    UUID inode;
    FileType file_type;
    status = ns.lookup("/a", &inode, &file_type);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(inode, a);
    ASSERT_EQ(file_type, FileType::DIRECTORY);
    entries.clear();
    ns.list(inode, &entries);
    ASSERT_EQ(entries.size(), 3);

    for (const auto& entry : entries) {
        if (entry.name == "d") {
            ASSERT_EQ(entry.type, FileType::FILE);
            ASSERT_EQ(entry.inode, d);
        } else if (entry.name == "e") {
            ASSERT_EQ(entry.type, FileType::FILE);
            ASSERT_EQ(entry.inode, e);
        } else if (entry.name == "f") {
            ASSERT_EQ(entry.type, FileType::FILE);
            ASSERT_EQ(entry.inode, f);
        } else {
            ASSERT_FALSE(true) << "unexpected entry: " << entry.name;
        }
    }
}
