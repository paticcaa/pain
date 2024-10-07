#include <gtest/gtest.h>
#include <filesystem>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <fmt/std.h>

#include "common/rocksdb_store.h"

using namespace pain;
using namespace pain::common;

class RocksdbStoreTest : public testing::Test {
protected:
    void SetUp() override {
        std::filesystem::remove_all("./test_rocksdb");
        std::filesystem::remove_all("./test_rocksdb_cpt");
    }

    void TearDown() override {
        // DO NOTHING
    }

    void create_data(RocksdbStorePtr store) {
        auto status = store->hset("pain", "name", "luffy");
        ASSERT_TRUE(status.ok()) << status.error_str();
        status = store->hset("pain", "age", "17");
        ASSERT_TRUE(status.ok()) << status.error_str();
        status = store->hset("pain", "height", "175");
        ASSERT_TRUE(status.ok()) << status.error_str();
        status = store->hset("pain", "weight", "60");
        ASSERT_TRUE(status.ok()) << status.error_str();

        status = store->hset("deva", "name", "zoro");
        ASSERT_TRUE(status.ok()) << status.error_str();
        status = store->hset("deva", "age", "19");
        ASSERT_TRUE(status.ok()) << status.error_str();
        status = store->hset("deva", "height", "180");
        ASSERT_TRUE(status.ok()) << status.error_str();
        status = store->hset("deva", "weight", "70");
        ASSERT_TRUE(status.ok()) << status.error_str();
    }

    void check(RocksdbStorePtr store) {
        std::string value;
        auto status = store->hget("pain", "name", &value);
        ASSERT_TRUE(status.ok()) << status.error_str();
        ASSERT_EQ(value, "luffy");
        status = store->hget("pain", "age", &value);
        ASSERT_TRUE(status.ok()) << status.error_str();
        ASSERT_EQ(value, "17");
        status = store->hget("pain", "height", &value);
        ASSERT_TRUE(status.ok()) << status.error_str();
        ASSERT_EQ(value, "175");
        status = store->hget("pain", "weight", &value);
        ASSERT_TRUE(status.ok()) << status.error_str();
        ASSERT_EQ(value, "60");

        status = store->hget("deva", "name", &value);
        ASSERT_TRUE(status.ok()) << status.error_str();
        ASSERT_EQ(value, "zoro");
        status = store->hget("deva", "age", &value);
        ASSERT_TRUE(status.ok()) << status.error_str();
        ASSERT_EQ(value, "19");
        status = store->hget("deva", "height", &value);
        ASSERT_TRUE(status.ok()) << status.error_str();
        ASSERT_EQ(value, "180");
        status = store->hget("deva", "weight", &value);
        ASSERT_TRUE(status.ok()) << status.error_str();
        ASSERT_EQ(value, "70");

        size_t len = 0;
        status = store->hlen("pain", &len);
        ASSERT_TRUE(status.ok()) << status.error_str();
        ASSERT_EQ(len, 4);
        status = store->hlen("deva", &len);
        ASSERT_TRUE(status.ok()) << status.error_str();
        ASSERT_EQ(len, 4);
    }
};

TEST_F(RocksdbStoreTest, open) {
    RocksdbStorePtr store;
    auto status = RocksdbStore::open("./test_rocksdb", &store);
    ASSERT_TRUE(status.ok()) << status.error_str();
    store->close();
}

TEST_F(RocksdbStoreTest, hsetget) {
    RocksdbStorePtr store;
    auto status = RocksdbStore::open("./test_rocksdb", &store);
    ASSERT_TRUE(status.ok()) << status.error_str();
    create_data(store);
    check(store);
    store->close();
}

TEST_F(RocksdbStoreTest, hexists) {
    RocksdbStorePtr store;
    auto status = RocksdbStore::open("./test_rocksdb", &store);
    ASSERT_TRUE(status.ok()) << status.error_str();
    create_data(store);
    check(store);
    bool exists = store->hexists("pain", "name");
    ASSERT_TRUE(exists);
    exists = store->hexists("pain", "not_exists");
    ASSERT_FALSE(exists);
    store->close();
}

TEST_F(RocksdbStoreTest, check_point) {
    RocksdbStorePtr store;
    auto status = RocksdbStore::open("./test_rocksdb", &store);
    ASSERT_TRUE(status.ok()) << status.error_str();
    create_data(store);
    check(store);
    std::vector<std::string> files;
    status = store->check_point("./test_rocksdb_cpt", &files);
    ASSERT_TRUE(status.ok()) << status.error_str();
    fmt::println("{}", fmt::join(files, ", "));
    check(store);

    status = store->hset("sad", "name", "sanji");
    EXPECT_TRUE(status.ok()) << status.error_str();
    status = store->hset("sad", "age", "20");
    EXPECT_TRUE(status.ok()) << status.error_str();
    status = store->hset("sad", "height", "180");
    EXPECT_TRUE(status.ok()) << status.error_str();
    status = store->hset("sad", "weight", "70");
    EXPECT_TRUE(status.ok()) << status.error_str();

    size_t len = 0;
    status = store->hlen("sad", &len);
    EXPECT_TRUE(status.ok()) << status.error_str();

    std::string value;
    status = store->hget("sad", "name", &value);
    EXPECT_TRUE(status.ok()) << status.error_str();
    EXPECT_EQ(value, "sanji");
    status = store->hget("sad", "age", &value);
    EXPECT_TRUE(status.ok()) << status.error_str();
    EXPECT_EQ(value, "20");
    status = store->hget("sad", "height", &value);
    EXPECT_TRUE(status.ok()) << status.error_str();
    EXPECT_EQ(value, "180");
    status = store->hget("sad", "weight", &value);
    EXPECT_TRUE(status.ok()) << status.error_str();
    EXPECT_EQ(value, "70");

    status = store->recover("./test_rocksdb_cpt");
    EXPECT_TRUE(status.ok()) << status.error_str();

    len = 0;
    status = store->hlen("sad", &len);
    EXPECT_TRUE(status.ok()) << status.error_str();
    EXPECT_EQ(len, 0);

    store->close();
}
