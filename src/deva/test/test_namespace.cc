#include <gtest/gtest.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <fmt/std.h>

#include <pain/base/path.h>
#include "common/rocksdb_store.h"
#include "common/txn_manager.h"
#include "deva/namespace.h"

using namespace pain;
using namespace pain::deva;

class TestNamespace : public ::testing::Test {
protected:
    void SetUp() override {
        _data_path = "/tmp/test_namespace_XXXXXX";
        make_temp_dir_or_die(&_data_path);
        common::RocksdbStorePtr store;
        auto status = common::RocksdbStore::open(_data_path.c_str(), &store);
        ASSERT_TRUE(status.ok()) << status.error_str();
        _store = store;
    }

    void TearDown() override {
        _store->close();
        std::filesystem::remove_all(_data_path);
    }

private:
    std::string _data_path;
    common::StorePtr _store;
};

TEST_F(TestNamespace, create) {
    Namespace ns(_store);
    ObjectId a = ObjectId::from_str_or_die("00000000-00000000-0000-0000-0000-000000000001");
    ObjectId b = ObjectId::from_str_or_die("00000000-00000000-0000-0000-0000-000000000002");
    ObjectId c = ObjectId::from_str_or_die("00000000-00000000-0000-0000-0000-000000000003");
    ObjectId d = ObjectId::from_str_or_die("00000000-00000000-0000-0000-0000-000000000004");
    ObjectId e = ObjectId::from_str_or_die("00000000-00000000-0000-0000-0000-000000000005");
    ObjectId f = ObjectId::from_str_or_die("00000000-00000000-0000-0000-0000-000000000006");
    auto status = ns.create(ns.root(), "a", FileType::kDirectory, a);
    ASSERT_TRUE(status.ok()) << status.error_str();
    status = ns.create(ns.root(), "b", FileType::kDirectory, b);
    ASSERT_TRUE(status.ok()) << status.error_str();
    status = ns.create(ns.root(), "c", FileType::kDirectory, c);
    ASSERT_TRUE(status.ok()) << status.error_str();

    std::list<DirEntry> entries;
    ns.list(ns.root(), &entries);
    for (const auto& entry : entries) {
        std::cout << entry.name << " " << entry.inode << std::endl;
    }
    ASSERT_EQ(entries.size(), 3);

    ns.create(a, "d", FileType::kDirectory, d);
    ns.create(a, "e", FileType::kDirectory, e);
    ns.create(a, "f", FileType::kDirectory, f);

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

TEST_F(TestNamespace, parse_path) {
    Namespace ns(_store);
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

TEST_F(TestNamespace, lookup) {
    ObjectId a = ObjectId::from_str_or_die("00000000-00000000-0000-0000-0000-000000000001");
    ObjectId b = ObjectId::from_str_or_die("00000000-00000000-0000-0000-0000-000000000002");
    ObjectId c = ObjectId::from_str_or_die("00000000-00000000-0000-0000-0000-000000000003");
    ObjectId d = ObjectId::from_str_or_die("00000000-00000000-0000-0000-0000-000000000004");
    ObjectId e = ObjectId::from_str_or_die("00000000-00000000-0000-0000-0000-000000000005");
    ObjectId f = ObjectId::from_str_or_die("00000000-00000000-0000-0000-0000-000000000006");

    Namespace ns(_store);
    ns.create(ns.root(), "a", FileType::kDirectory, a);
    ns.create(ns.root(), "b", FileType::kDirectory, b);
    ns.create(ns.root(), "c", FileType::kDirectory, c);
    ns.create(a, "d", FileType::kDirectory, d);
    ns.create(a, "e", FileType::kDirectory, e);
    ns.create(a, "f", FileType::kFile, f);

    ObjectId inode;
    FileType file_type = FileType::kFile;
    auto status = ns.lookup("/", &inode, &file_type);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(inode, ns.root());
    ASSERT_EQ(file_type, FileType::kDirectory);

    status = ns.lookup("/a", &inode, &file_type);
    ASSERT_TRUE(status.ok()) << status.error_str();
    ASSERT_EQ(inode, a);
    ASSERT_EQ(file_type, FileType::kDirectory);

    status = ns.lookup("/b", &inode, &file_type);
    ASSERT_TRUE(status.ok()) << status.error_str();
    ASSERT_EQ(inode, b);
    ASSERT_EQ(file_type, FileType::kDirectory);

    status = ns.lookup("/c", &inode, &file_type);
    ASSERT_TRUE(status.ok()) << status.error_str();
    ASSERT_EQ(inode, c);
    ASSERT_EQ(file_type, FileType::kDirectory);

    status = ns.lookup("/a/d", &inode, &file_type);
    ASSERT_TRUE(status.ok()) << status.error_str();
    ASSERT_EQ(inode, d);
    ASSERT_EQ(file_type, FileType::kDirectory);

    status = ns.lookup("/a/e", &inode, &file_type);
    ASSERT_TRUE(status.ok()) << status.error_str();
    ASSERT_EQ(inode, e);
    ASSERT_EQ(file_type, FileType::kDirectory);

    status = ns.lookup("/a/f", &inode, &file_type);
    ASSERT_TRUE(status.ok()) << status.error_str();
    ASSERT_EQ(inode, f);
    ASSERT_EQ(file_type, FileType::kFile);

    status = ns.lookup("/a/g", &inode, &file_type);
    ASSERT_FALSE(status.ok()) << status.error_str();

    status = ns.lookup("/a/f/b", &inode, &file_type);
    ASSERT_FALSE(status.ok()) << status.error_str();
}

TEST_F(TestNamespace, lookup_and_list) {
    Namespace ns(_store);
    ObjectId a = ObjectId::from_str_or_die("00000000-00000000-0000-0000-0000-000000000001");
    ObjectId b = ObjectId::from_str_or_die("00000000-00000000-0000-0000-0000-000000000002");
    ObjectId c = ObjectId::from_str_or_die("00000000-00000000-0000-0000-0000-000000000003");
    ObjectId d = ObjectId::from_str_or_die("00000000-00000000-0000-0000-0000-000000000004");
    ObjectId e = ObjectId::from_str_or_die("00000000-00000000-0000-0000-0000-000000000005");
    ObjectId f = ObjectId::from_str_or_die("00000000-00000000-0000-0000-0000-000000000006");
    auto status = ns.create(ns.root(), "a", FileType::kDirectory, a);
    ASSERT_TRUE(status.ok()) << status.error_str();
    status = ns.create(ns.root(), "b", FileType::kDirectory, b);
    ASSERT_TRUE(status.ok()) << status.error_str();
    status = ns.create(ns.root(), "c", FileType::kDirectory, c);
    ASSERT_TRUE(status.ok()) << status.error_str();

    std::list<DirEntry> entries;
    ns.list(ns.root(), &entries);
    ASSERT_EQ(entries.size(), 3) << fmt::format("{}", fmt::join(entries, ", "));

    ns.create(a, "d", FileType::kFile, d);
    ns.create(a, "e", FileType::kFile, e);
    ns.create(a, "f", FileType::kFile, f);

    ObjectId inode;
    FileType file_type = FileType::kFile;
    status = ns.lookup("/a", &inode, &file_type);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(inode, a);
    ASSERT_EQ(file_type, FileType::kDirectory);
    entries.clear();
    ns.list(inode, &entries);
    ASSERT_EQ(entries.size(), 3);

    for (const auto& entry : entries) {
        if (entry.name == "d") {
            ASSERT_EQ(entry.type, FileType::kFile);
            ASSERT_EQ(entry.inode, d);
        } else if (entry.name == "e") {
            ASSERT_EQ(entry.type, FileType::kFile);
            ASSERT_EQ(entry.inode, e);
        } else if (entry.name == "f") {
            ASSERT_EQ(entry.type, FileType::kFile);
            ASSERT_EQ(entry.inode, f);
        } else {
            ASSERT_FALSE(true) << "unexpected entry: " << entry.name;
        }
    }
}

TEST_F(TestNamespace, lookup_and_list_in_txn) {
    Namespace ns(_store);
    ObjectId a = ObjectId::from_str_or_die("00000000-00000000-0000-0000-0000-000000000001");
    ObjectId b = ObjectId::from_str_or_die("00000000-00000000-0000-0000-0000-000000000002");
    ObjectId c = ObjectId::from_str_or_die("00000000-00000000-0000-0000-0000-000000000003");
    ObjectId d = ObjectId::from_str_or_die("00000000-00000000-0000-0000-0000-000000000004");
    ObjectId e = ObjectId::from_str_or_die("00000000-00000000-0000-0000-0000-000000000005");
    ObjectId f = ObjectId::from_str_or_die("00000000-00000000-0000-0000-0000-000000000006");
    auto txn = _store->begin_txn();
    PAIN_TXN(txn.get()) {
        auto status = ns.create(ns.root(), "a", FileType::kDirectory, a);
        EXPECT_TRUE(status.ok()) << status.error_str();
        status = ns.create(ns.root(), "b", FileType::kDirectory, b);
        EXPECT_TRUE(status.ok()) << status.error_str();
        status = ns.create(ns.root(), "c", FileType::kDirectory, c);
        EXPECT_TRUE(status.ok()) << status.error_str();
        return status;
    };

    std::list<DirEntry> entries;
    ns.list(ns.root(), &entries);
    ASSERT_EQ(entries.size(), 3) << fmt::format("{}", fmt::join(entries, ", "));
    EXPECT_EQ(entries.front().name, "a");
    entries.pop_front();
    EXPECT_EQ(entries.front().name, "b");
    entries.pop_front();
    EXPECT_EQ(entries.front().name, "c");
    entries.pop_front();
    EXPECT_EQ(entries.size(), 0);
}
