#include <gtest/gtest.h>

#include "deva/namespace.h"

using namespace pain;
using namespace pain::deva;

TEST(Namespace, create) {
    Namespace& ns = Namespace::instance();
    ns.create(ns.root(), "a", FileType::DIRECTORY, UUID::from_str_or_die("00000000-0000-0000-0000-000000000001"));
    ns.create(ns.root(), "b", FileType::DIRECTORY, UUID::from_str_or_die("00000000-0000-0000-0000-000000000002"));
    ns.create(ns.root(), "c", FileType::DIRECTORY, UUID::from_str_or_die("00000000-0000-0000-0000-000000000003"));

    std::list<DirEntry> entries;
    ns.list(ns.root(), &entries);
    ASSERT_EQ(entries.size(), 3);

    ns.create(UUID::from_str_or_die("00000000-0000-0000-0000-000000000001"),
              "d",
              FileType::DIRECTORY,
              UUID::from_str_or_die("00000000-0000-0000-0000-000000000004"));
    ns.create(UUID::from_str_or_die("00000000-0000-0000-0000-000000000001"),
              "e",
              FileType::DIRECTORY,
              UUID::from_str_or_die("00000000-0000-0000-0000-000000000005"));
    ns.create(UUID::from_str_or_die("00000000-0000-0000-0000-000000000001"),
              "f",
              FileType::DIRECTORY,
              UUID::from_str_or_die("00000000-0000-0000-0000-000000000006"));

    ns.list(UUID::from_str_or_die("00000000-0000-0000-0000-000000000001"), &entries);
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
    Namespace& ns = Namespace::instance();
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
