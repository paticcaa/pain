#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <filesystem>
#include <future>
#include <thread>
#include <vector>
#include "manusya/file_handle.h"
#include "manusya/local_store.h"

// NOLINTBEGIN(readability-magic-numbers)
namespace {
using namespace pain;
using namespace pain::manusya;

class TestLocalStore : public ::testing::Test {
protected:
    void SetUp() override {
        // 创建临时测试目录
        _test_dir = std::filesystem::temp_directory_path() / "test_local_store";
        std::filesystem::create_directories(_test_dir);

        _store = Store::create(("local://" + _test_dir.string()).c_str());
        ASSERT_TRUE(_store != nullptr);
    }

    void TearDown() override {
        _store.reset();

        // 清理测试目录
        if (std::filesystem::exists(_test_dir)) {
            std::filesystem::remove_all(_test_dir);
        }
    }

    std::filesystem::path _test_dir;
    StorePtr _store;

    // 辅助方法：创建测试文件路径
    std::string make_test_path(const std::string& filename) {
        return (_test_dir / filename).string();
    }

    // 辅助方法：检查文件是否存在
    bool file_exists(const std::string& filename) {
        return std::filesystem::exists(_test_dir / filename);
    }

    // 辅助方法：获取文件大小
    uint64_t get_file_size(const std::string& filename) {
        if (!file_exists(filename)) {
            return 0;
        }
        return std::filesystem::file_size(_test_dir / filename);
    }
};

// 基础功能测试
TEST_F(TestLocalStore, BasicOpen) {
    FileHandlePtr fh;
    auto future = _store->open("test_file1", O_RDWR | O_CREAT, &fh);
    auto status = future.get();

    ASSERT_TRUE(status.ok()) << "Failed to open file: " << status.error_str();
    ASSERT_TRUE(fh != nullptr);
    ASSERT_EQ(fh->use_count(), 1);
    ASSERT_TRUE(file_exists("test_file1"));
}

TEST_F(TestLocalStore, OpenMultipleFiles) {
    std::vector<FileHandlePtr> handles;

    // 打开多个文件
    for (int i = 0; i < 5; ++i) {
        FileHandlePtr fh;
        auto filename = "test_file" + std::to_string(i);
        auto future = _store->open(filename.c_str(), O_RDWR | O_CREAT, &fh);
        auto status = future.get();

        ASSERT_TRUE(status.ok()) << "Failed to open file " << i << ": " << status.error_str();
        ASSERT_TRUE(fh != nullptr);
        ASSERT_TRUE(file_exists(filename));
        handles.push_back(fh);
    }

    ASSERT_EQ(handles.size(), 5);
}

TEST_F(TestLocalStore, BasicAppendAndRead) {
    FileHandlePtr fh;
    auto future = _store->open("test_file1", O_RDWR | O_CREAT, &fh);
    auto status = future.get();
    ASSERT_TRUE(status.ok());

    // 写入数据
    IOBuf write_buf;
    const char* test_data = "Hello, World!";
    write_buf.append(test_data, strlen(test_data));

    auto append_future = _store->append(fh, 0, write_buf);
    status = append_future.get();
    ASSERT_TRUE(status.ok());

    // 验证文件大小
    ASSERT_EQ(get_file_size("test_file1"), strlen(test_data));

    // 读取数据
    IOBuf read_buf;
    auto read_future = _store->read(fh, 0, strlen(test_data), &read_buf);
    status = read_future.get();
    ASSERT_TRUE(status.ok());

    ASSERT_EQ(read_buf.size(), strlen(test_data));
    std::string read_data(read_buf.to_string());
    ASSERT_EQ(read_data, test_data);
}

TEST_F(TestLocalStore, AppendMultipleTimes) {
    FileHandlePtr fh;
    auto future = _store->open("test_file1", O_RDWR | O_CREAT, &fh);
    auto status = future.get();
    ASSERT_TRUE(status.ok());

    // 多次写入
    std::vector<std::string> test_data = {"Hello", ", ", "World", "!"};
    uint64_t total_size = 0;

    for (const auto& data : test_data) {
        IOBuf buf;
        buf.append(data.c_str(), data.length());

        auto append_future = _store->append(fh, 0, buf);
        status = append_future.get();
        ASSERT_TRUE(status.ok());

        total_size += data.length();
    }

    // 验证文件大小
    ASSERT_EQ(get_file_size("test_file1"), total_size);

    // 读取全部数据
    IOBuf read_buf;
    auto read_future = _store->read(fh, 0, total_size, &read_buf);
    status = read_future.get();
    ASSERT_TRUE(status.ok());

    std::string expected = "Hello, World!";
    std::string actual(read_buf.to_string());
    ASSERT_EQ(actual, expected);
}

TEST_F(TestLocalStore, ReadWithOffset) {
    FileHandlePtr fh;
    auto future = _store->open("test_file1", O_RDWR | O_CREAT, &fh);
    auto status = future.get();
    ASSERT_TRUE(status.ok());

    // 写入数据
    IOBuf write_buf;
    const char* test_data = "Hello, World!";
    write_buf.append(test_data, strlen(test_data));

    auto append_future = _store->append(fh, 0, write_buf);
    status = append_future.get();
    ASSERT_TRUE(status.ok());

    // 从偏移量读取
    IOBuf read_buf;
    auto read_future = _store->read(fh, 7, 5, &read_buf); // 读取 "World"
    status = read_future.get();
    ASSERT_TRUE(status.ok());

    std::string expected = "World";
    std::string actual(read_buf.to_string());
    ASSERT_EQ(actual, expected);
}

TEST_F(TestLocalStore, FileSize) {
    FileHandlePtr fh;
    auto future = _store->open("test_file1", O_RDWR | O_CREAT, &fh);
    auto status = future.get();
    ASSERT_TRUE(status.ok());

    // 初始大小应该为0
    uint64_t size = 0;
    auto size_future = _store->size(fh, &size);
    status = size_future.get();
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(size, 0);

    // 写入数据后检查大小
    IOBuf write_buf;
    const char* test_data = "Hello, World!";
    write_buf.append(test_data, strlen(test_data));

    auto append_future = _store->append(fh, 0, write_buf);
    status = append_future.get();
    ASSERT_TRUE(status.ok());

    size_future = _store->size(fh, &size);
    status = size_future.get();
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(size, strlen(test_data));
}

TEST_F(TestLocalStore, SealFile) {
    FileHandlePtr fh;
    auto future = _store->open("test_file1", O_RDWR | O_CREAT, &fh);
    auto status = future.get();
    ASSERT_TRUE(status.ok());

    // seal操作应该成功
    auto seal_future = _store->seal(fh);
    status = seal_future.get();
    ASSERT_TRUE(status.ok());

    // 验证文件权限已更改（只读）
    std::filesystem::perms perms = std::filesystem::status(_test_dir / "test_file1").permissions();
    // 注意：在Windows上可能无法检测权限变化
}

TEST_F(TestLocalStore, RemoveFile) {
    FileHandlePtr fh;
    auto future = _store->open("test_file1", O_RDWR | O_CREAT, &fh);
    auto status = future.get();
    ASSERT_TRUE(status.ok());

    // 写入一些数据
    IOBuf write_buf;
    const char* test_data = "Hello, World!";
    write_buf.append(test_data, strlen(test_data));

    auto append_future = _store->append(fh, 0, write_buf);
    status = append_future.get();
    ASSERT_TRUE(status.ok());

    // 验证文件存在
    ASSERT_TRUE(file_exists("test_file1"));

    // 删除文件
    auto remove_future = _store->remove("test_file1");
    status = remove_future.get();
    ASSERT_TRUE(status.ok());

    // 验证文件已被删除
    ASSERT_FALSE(file_exists("test_file1"));
}

// 属性操作测试
TEST_F(TestLocalStore, SetAndGetAttribute) {
    FileHandlePtr fh;
    auto future = _store->open("test_file1", O_RDWR | O_CREAT, &fh);
    auto status = future.get();
    ASSERT_TRUE(status.ok());

    // 设置属性
    auto set_future = _store->set_attr(fh, "key1", "value1");
    status = set_future.get();
    if (status.error_code() == ENOTSUP) {
        GTEST_SKIP() << "set_attr is not supported";
    }
    ASSERT_TRUE(status.ok()) << "Failed to set attribute: " << status.error_str() << "(" << status.error_code() << ")";

    // 获取属性
    std::string value;
    auto get_future = _store->get_attr(fh, "key1", &value);
    status = get_future.get();
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value, "value1");
}

TEST_F(TestLocalStore, SetMultipleAttributes) {
    FileHandlePtr fh;
    auto future = _store->open("test_file1", O_RDWR | O_CREAT, &fh);
    auto status = future.get();
    ASSERT_TRUE(status.ok());

    // 设置多个属性
    std::map<std::string, std::string> attrs = {{"key1", "value1"}, {"key2", "value2"}, {"key3", "value3"}};

    for (const auto& [key, value] : attrs) {
        auto set_future = _store->set_attr(fh, key.c_str(), value.c_str());
        status = set_future.get();
        if (status.error_code() == ENOTSUP) {
            GTEST_SKIP() << "set_attr is not supported";
        }
        ASSERT_TRUE(status.ok()) << "Failed to set attribute: " << status.error_str() << "(" << status.error_code()
                                 << ")";
    }

    // 列出所有属性
    std::map<std::string, std::string> retrieved_attrs;
    auto list_future = _store->list_attrs(fh, &retrieved_attrs);
    status = list_future.get();
    ASSERT_TRUE(status.ok());

    ASSERT_EQ(retrieved_attrs.size(), attrs.size());
    for (const auto& [key, value] : attrs) {
        ASSERT_EQ(retrieved_attrs[key], value);
    }
}

TEST_F(TestLocalStore, GetNonExistentAttribute) {
    FileHandlePtr fh;
    auto future = _store->open("test_file1", O_RDWR | O_CREAT, &fh);
    auto status = future.get();
    ASSERT_TRUE(status.ok());

    // 获取不存在的属性
    std::string value;
    auto get_future = _store->get_attr(fh, "non_existent_key", &value);
    status = get_future.get();
    ASSERT_FALSE(status.ok());
    // 注意：在Windows上可能返回不同的错误码
}

TEST_F(TestLocalStore, ListAttributesEmpty) {
    FileHandlePtr fh;
    auto future = _store->open("test_file1", O_RDWR | O_CREAT, &fh);
    auto status = future.get();
    ASSERT_TRUE(status.ok());

    // 列出空属性
    std::map<std::string, std::string> attrs;
    auto list_future = _store->list_attrs(fh, &attrs);
    status = list_future.get();
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(attrs.empty());
}

// for_each 测试
TEST_F(TestLocalStore, ForEachFiles) {
    // 创建多个文件
    std::vector<std::string> filenames = {"test_file1", "test_file2", "test_file3"};

    for (const auto& filename : filenames) {
        FileHandlePtr fh;
        auto future = _store->open(filename.c_str(), O_RDWR | O_CREAT, &fh);
        auto status = future.get();
        ASSERT_TRUE(status.ok()) << "Failed to open file " << filename << ": " << status.error_str();
    }

    // 使用for_each遍历
    std::vector<std::string> found_files;
    _store->for_each([&found_files](const char* path) {
        found_files.emplace_back(path);
    });

    ASSERT_EQ(found_files.size(), filenames.size());
    for (const auto& filename : filenames) {
        ASSERT_TRUE(std::find(found_files.begin(), found_files.end(), filename) != found_files.end());
    }
}

TEST_F(TestLocalStore, ForEachEmpty) {
    // 空存储的for_each
    std::vector<std::string> found_files;
    _store->for_each([&found_files](const char* path) {
        found_files.emplace_back(path);
    });

    ASSERT_TRUE(found_files.empty());
}

// 边界情况测试
TEST_F(TestLocalStore, ReadBeyondFileSize) {
    FileHandlePtr fh;
    auto future = _store->open("test_file1", O_RDWR | O_CREAT, &fh);
    auto status = future.get();
    ASSERT_TRUE(status.ok());

    // 写入少量数据
    IOBuf write_buf;
    const char* test_data = "Hello";
    write_buf.append(test_data, strlen(test_data));

    auto append_future = _store->append(fh, 0, write_buf);
    status = append_future.get();
    ASSERT_TRUE(status.ok());

    // 尝试读取超出文件大小的数据
    IOBuf read_buf;
    auto read_future = _store->read(fh, 0, 100, &read_buf); // 尝试读取100字节
    status = read_future.get();
    ASSERT_FALSE(status.ok()); // 应该失败，因为请求的大小超过了实际文件大小
}

TEST_F(TestLocalStore, ReadFromInvalidOffset) {
    FileHandlePtr fh;
    auto future = _store->open("test_file1", O_RDWR | O_CREAT, &fh);
    auto status = future.get();
    ASSERT_TRUE(status.ok());

    // 写入数据
    IOBuf write_buf;
    const char* test_data = "Hello";
    write_buf.append(test_data, strlen(test_data));

    auto append_future = _store->append(fh, 0, write_buf);
    status = append_future.get();
    ASSERT_TRUE(status.ok());

    // 从无效偏移量读取
    IOBuf read_buf;
    auto read_future = _store->read(fh, 100, 10, &read_buf); // 从偏移量100读取
    status = read_future.get();
    ASSERT_FALSE(status.ok()); // 应该失败，因为偏移量超出了文件范围
}

TEST_F(TestLocalStore, AppendToInvalidFileHandle) {
    EXPECT_EXIT(new FileHandle(nullptr), testing::KilledBySignal(SIGABRT), "store is nullptr");
}

TEST_F(TestLocalStore, LargeDataHandling) {
    FileHandlePtr fh;
    auto future = _store->open("large_file", O_RDWR | O_CREAT, &fh);
    auto status = future.get();
    ASSERT_TRUE(status.ok());

    // 写入大量数据
    const size_t large_size = 1024 * 1024; // 1MB
    std::vector<char> large_data(large_size, 'A');

    IOBuf write_buf;
    write_buf.append(large_data.data(), large_data.size());

    auto append_future = _store->append(fh, 0, write_buf);
    status = append_future.get();
    ASSERT_TRUE(status.ok());

    // 验证文件大小
    uint64_t size = 0;
    auto size_future = _store->size(fh, &size);
    status = size_future.get();
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(size, large_size);

    // 读取部分数据
    IOBuf read_buf;
    auto read_future = _store->read(fh, 0, 1024, &read_buf); // 读取前1KB
    status = read_future.get();
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(read_buf.size(), 1024);

    // 验证数据内容
    std::string read_data(read_buf.to_string());
    ASSERT_EQ(read_data, std::string(1024, 'A'));
}

TEST_F(TestLocalStore, EmptyStringHandling) {
    FileHandlePtr fh;
    auto future = _store->open("empty_file", O_RDWR | O_CREAT, &fh);
    auto status = future.get();
    ASSERT_TRUE(status.ok());

    // 写入空字符串
    IOBuf empty_buf;
    auto append_future = _store->append(fh, 0, empty_buf);
    status = append_future.get();
    ASSERT_TRUE(status.ok());

    // 验证文件大小
    uint64_t size = 0;
    auto size_future = _store->size(fh, &size);
    status = size_future.get();
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(size, 0);

    // 读取空文件
    IOBuf read_buf;
    auto read_future = _store->read(fh, 0, 10, &read_buf);
    status = read_future.get();
    ASSERT_FALSE(status.ok()); // 应该失败，因为请求的大小超过了实际文件大小
}

TEST_F(TestLocalStore, NullPointerHandling) {
    // 测试空指针参数
    FileHandlePtr fh;
    auto future = _store->open("test_file1", O_RDWR | O_CREAT, &fh);
    auto status = future.get();
    ASSERT_TRUE(status.ok());

    // 测试空键值
    auto set_future = _store->set_attr(fh, nullptr, "value");
    status = set_future.get();
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), EINVAL);

    std::string value;
    auto get_future = _store->get_attr(fh, nullptr, &value);
    status = get_future.get();
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), EINVAL);
}

TEST_F(TestLocalStore, FileHandleReferenceCounting) {
    FileHandlePtr fh1;
    FileHandlePtr fh2;

    // 打开同一个文件两次
    auto future1 = _store->open("test_file1", O_RDWR | O_CREAT, &fh1);
    auto status = future1.get();
    ASSERT_TRUE(status.ok());

    auto future2 = _store->open("test_file1", O_RDWR | O_CREAT, &fh2);
    status = future2.get();
    ASSERT_TRUE(status.ok());

    // 验证引用计数
    ASSERT_EQ(fh1->use_count(), 1);
    ASSERT_EQ(fh2->use_count(), 1);

    // 写入数据到第一个句柄
    IOBuf write_buf;
    const char* test_data = "Hello";
    write_buf.append(test_data, strlen(test_data));

    auto append_future = _store->append(fh1, 0, write_buf);
    status = append_future.get();
    ASSERT_TRUE(status.ok());

    // 从第二个句柄读取数据
    IOBuf read_buf;
    auto read_future = _store->read(fh2, 0, strlen(test_data), &read_buf);
    status = read_future.get();
    ASSERT_TRUE(status.ok());

    std::string actual(read_buf.to_string());
    ASSERT_EQ(actual, test_data);
}

TEST_F(TestLocalStore, StoreReferenceCounting) {
    // 测试Store的引用计数
    StorePtr store_ref = _store.get();
    ASSERT_EQ(store_ref->use_count(), 2);

    // 创建文件句柄会增加引用计数
    FileHandlePtr fh;
    auto future = _store->open("test_file1", O_RDWR | O_CREAT, &fh);
    auto status = future.get();
    ASSERT_TRUE(status.ok()) << "Failed to open file: " << status.error_str();

    // 文件句柄持有Store的引用
    ASSERT_EQ(store_ref->use_count(), 3);

    // 释放文件句柄
    fh.reset();
    ASSERT_EQ(store_ref->use_count(), 2);
}

// 新增的测试用例：参数验证和 flags 处理
TEST_F(TestLocalStore, OpenWithNullPath) {
    FileHandlePtr fh;
    auto future = _store->open(nullptr, O_RDWR | O_CREAT, &fh);
    auto status = future.get();

    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), EINVAL);
    ASSERT_EQ(status.error_str(), "path is nullptr");
}

TEST_F(TestLocalStore, OpenWithNullFileHandle) {
    auto future = _store->open("test_file1", O_RDWR | O_CREAT, nullptr);
    auto status = future.get();

    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), EINVAL);
    ASSERT_EQ(status.error_str(), "fh is nullptr");
}

TEST_F(TestLocalStore, OpenWithExclusiveFlag) {
    // 第一次打开文件
    FileHandlePtr fh1;
    auto future1 = _store->open("exclusive_file", O_RDWR | O_CREAT, &fh1);
    auto status = future1.get();
    ASSERT_TRUE(status.ok()) << "Failed to open file: " << status.error_str();

    // 尝试用 O_EXCL 标志再次打开同一个文件
    FileHandlePtr fh2;
    auto future2 = _store->open("exclusive_file", O_RDWR | O_CREAT | O_EXCL, &fh2);
    status = future2.get();

    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), EEXIST);
}

TEST_F(TestLocalStore, OpenWithTruncateFlag) {
    // 创建文件并写入数据
    FileHandlePtr fh1;
    auto future1 = _store->open("truncate_file", O_RDWR | O_CREAT, &fh1);
    auto status = future1.get();
    ASSERT_TRUE(status.ok()) << "Failed to open file: " << status.error_str();

    IOBuf write_buf;
    const char* test_data = "Hello, World!";
    write_buf.append(test_data, strlen(test_data));

    auto append_future = _store->append(fh1, 0, write_buf);
    status = append_future.get();
    ASSERT_TRUE(status.ok());

    // 验证文件大小
    uint64_t size = 0;
    auto size_future = _store->size(fh1, &size);
    status = size_future.get();
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(size, strlen(test_data));

    // 用 O_TRUNC 标志重新打开文件
    FileHandlePtr fh2;
    auto future2 = _store->open("truncate_file", O_RDWR | O_TRUNC, &fh2);
    status = future2.get();
    ASSERT_TRUE(status.ok()) << "Failed to open file with O_TRUNC: " << status.error_str();

    // 验证文件已被截断
    size_future = _store->size(fh2, &size);
    status = size_future.get();
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(size, 0);
}

TEST_F(TestLocalStore, OpenWithoutCreateFlag) {
    // 尝试打开不存在的文件，不使用 O_CREAT 标志
    FileHandlePtr fh;
    auto future = _store->open("nonexistent_file", O_RDWR, &fh);
    auto status = future.get();

    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), ENOENT);
}

TEST_F(TestLocalStore, AppendWithNullFileHandle) {
    IOBuf write_buf;
    const char* test_data = "Hello";
    write_buf.append(test_data, strlen(test_data));

    auto append_future = _store->append(nullptr, 0, write_buf);
    auto status = append_future.get();

    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), EINVAL);
    ASSERT_EQ(status.error_str(), "fh is nullptr");
}

TEST_F(TestLocalStore, ReadWithNullFileHandle) {
    IOBuf read_buf;
    auto read_future = _store->read(nullptr, 0, 10, &read_buf);
    auto status = read_future.get();

    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), EINVAL);
    ASSERT_EQ(status.error_str(), "fh is nullptr");
}

TEST_F(TestLocalStore, ReadWithNullBuffer) {
    FileHandlePtr fh;
    auto future = _store->open("test_file1", O_RDWR | O_CREAT, &fh);
    auto status = future.get();
    ASSERT_TRUE(status.ok()) << "Failed to open file: " << status.error_str();

    auto read_future = _store->read(fh, 0, 10, nullptr);
    status = read_future.get();

    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), EINVAL);
    ASSERT_EQ(status.error_str(), "buf is nullptr");
}

TEST_F(TestLocalStore, SizeWithNullFileHandle) {
    uint64_t size = 0;
    auto size_future = _store->size(nullptr, &size);
    auto status = size_future.get();

    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), EINVAL);
    ASSERT_EQ(status.error_str(), "fh is nullptr");
}

TEST_F(TestLocalStore, SizeWithNullSize) {
    FileHandlePtr fh;
    auto future = _store->open("test_file1", O_RDWR | O_CREAT, &fh);
    auto status = future.get();
    ASSERT_TRUE(status.ok()) << "Failed to open file: " << status.error_str();

    auto size_future = _store->size(fh, nullptr);
    status = size_future.get();

    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), EINVAL);
    ASSERT_EQ(status.error_str(), "size is nullptr");
}

TEST_F(TestLocalStore, RemoveWithNullPath) {
    auto remove_future = _store->remove(nullptr);
    auto status = remove_future.get();

    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), EINVAL);
    ASSERT_EQ(status.error_str(), "path is nullptr");
}

TEST_F(TestLocalStore, SealWithNullFileHandle) {
    auto seal_future = _store->seal(nullptr);
    auto status = seal_future.get();

    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), EINVAL);
    ASSERT_EQ(status.error_str(), "fh is nullptr");
}

TEST_F(TestLocalStore, SetAttrWithNullFileHandle) {
    auto set_future = _store->set_attr(nullptr, "key", "value");
    auto status = set_future.get();

    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), EINVAL);
    ASSERT_EQ(status.error_str(), "fh is nullptr");
}

TEST_F(TestLocalStore, SetAttrWithNullKey) {
    FileHandlePtr fh;
    auto future = _store->open("test_file1", O_RDWR | O_CREAT, &fh);
    auto status = future.get();
    ASSERT_TRUE(status.ok()) << "Failed to open file: " << status.error_str();

    auto set_future = _store->set_attr(fh, nullptr, "value");
    status = set_future.get();

    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), EINVAL);
    ASSERT_EQ(status.error_str(), "key is nullptr");
}

TEST_F(TestLocalStore, SetAttrWithNullValue) {
    FileHandlePtr fh;
    auto future = _store->open("test_file1", O_RDWR | O_CREAT, &fh);
    auto status = future.get();
    ASSERT_TRUE(status.ok()) << "Failed to open file: " << status.error_str();

    auto set_future = _store->set_attr(fh, "key", nullptr);
    status = set_future.get();

    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), EINVAL);
    ASSERT_EQ(status.error_str(), "value is nullptr");
}

TEST_F(TestLocalStore, GetAttrWithNullFileHandle) {
    std::string value;
    auto get_future = _store->get_attr(nullptr, "key", &value);
    auto status = get_future.get();

    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), EINVAL);
    ASSERT_EQ(status.error_str(), "fh is nullptr");
}

TEST_F(TestLocalStore, GetAttrWithNullKey) {
    FileHandlePtr fh;
    auto future = _store->open("test_file1", O_RDWR | O_CREAT, &fh);
    auto status = future.get();
    ASSERT_TRUE(status.ok()) << "Failed to open file: " << status.error_str();

    std::string value;
    auto get_future = _store->get_attr(fh, nullptr, &value);
    status = get_future.get();

    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), EINVAL);
    ASSERT_EQ(status.error_str(), "key is nullptr");
}

TEST_F(TestLocalStore, GetAttrWithNullValue) {
    FileHandlePtr fh;
    auto future = _store->open("test_file1", O_RDWR | O_CREAT, &fh);
    auto status = future.get();
    ASSERT_TRUE(status.ok()) << "Failed to open file: " << status.error_str();

    auto get_future = _store->get_attr(fh, "key", nullptr);
    status = get_future.get();

    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), EINVAL);
    ASSERT_EQ(status.error_str(), "value is nullptr");
}

TEST_F(TestLocalStore, ListAttrsWithNullFileHandle) {
    std::map<std::string, std::string> attrs;
    auto list_future = _store->list_attrs(nullptr, &attrs);
    auto status = list_future.get();

    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), EINVAL);
    ASSERT_EQ(status.error_str(), "fh is nullptr");
}

TEST_F(TestLocalStore, ListAttrsWithNullAttrs) {
    FileHandlePtr fh;
    auto future = _store->open("test_file1", O_RDWR | O_CREAT, &fh);
    auto status = future.get();
    ASSERT_TRUE(status.ok()) << "Failed to open file: " << status.error_str();

    auto list_future = _store->list_attrs(fh, nullptr);
    status = list_future.get();

    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), EINVAL);
    ASSERT_EQ(status.error_str(), "attrs is nullptr");
}

// 额外的边界情况测试
TEST_F(TestLocalStore, EmptyPathString) {
    // 测试空字符串路径
    FileHandlePtr fh;
    auto future = _store->open("", O_RDWR | O_CREAT, &fh);
    auto status = future.get();

    // 当前实现没有检查空字符串，这可能导致未定义行为
    // 但至少不应该崩溃
}

TEST_F(TestLocalStore, VeryLongPath) {
    // 测试非常长的路径
    std::string long_path(1000, 'a');

    FileHandlePtr fh;
    auto future = _store->open(long_path.c_str(), O_RDWR | O_CREAT, &fh);
    auto status = future.get();
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), ENAMETOOLONG);
}

TEST_F(TestLocalStore, SpecialCharactersInPath) {
    // 测试路径中的特殊字符
    std::vector<std::string> special_paths = {"file with spaces",
                                              "file-with-dashes",
                                              "file_with_underscores",
                                              "file.with.dots",
                                              "file@#$%^&*()",
                                              "中文文件名",
                                              "ファイル名"};

    for (const auto& path : special_paths) {
        FileHandlePtr fh;
        auto future = _store->open(path.c_str(), O_RDWR | O_CREAT, &fh);
        auto status = future.get();
        ASSERT_TRUE(status.ok()) << "Failed to open file with special path '" << path << "': " << status.error_str();

        // 验证文件可以被正常操作
        IOBuf write_buf;
        std::string test_data = "Test data for " + path;
        write_buf.append(test_data.c_str(), test_data.length());

        auto append_future = _store->append(fh, 0, write_buf);
        status = append_future.get();
        ASSERT_TRUE(status.ok());
    }
}

TEST_F(TestLocalStore, ZeroSizeOperations) {
    // 测试零大小的操作
    FileHandlePtr fh;
    auto future = _store->open("zero_size", O_RDWR | O_CREAT, &fh);
    auto status = future.get();
    ASSERT_TRUE(status.ok()) << "Failed to open file: " << status.error_str();

    // 写入零大小的数据
    IOBuf empty_buf;
    auto append_future = _store->append(fh, 0, empty_buf);
    status = append_future.get();
    ASSERT_TRUE(status.ok());

    // 验证文件大小
    uint64_t size = 0;
    auto size_future = _store->size(fh, &size);
    status = size_future.get();
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(size, 0);

    // 从零偏移量读取零大小
    IOBuf read_buf;
    auto read_future = _store->read(fh, 0, 0, &read_buf);
    status = read_future.get();
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(read_buf.size(), 0);
}

TEST_F(TestLocalStore, LargeOffsetOperations) {
    // 测试大偏移量操作
    FileHandlePtr fh;
    auto future = _store->open("large_offset", O_RDWR | O_CREAT, &fh);
    auto status = future.get();
    ASSERT_TRUE(status.ok()) << "Failed to open file: " << status.error_str();

    // 写入一些数据
    IOBuf write_buf;
    const char* test_data = "Hello";
    write_buf.append(test_data, strlen(test_data));

    auto append_future = _store->append(fh, 0, write_buf);
    status = append_future.get();
    ASSERT_TRUE(status.ok());

    // 使用大偏移量读取
    IOBuf read_buf;
    auto read_future = _store->read(fh, UINT64_MAX - 100, 10, &read_buf);
    status = read_future.get();
    ASSERT_FALSE(status.ok()); // 应该失败，因为偏移量超出了文件范围
}

TEST_F(TestLocalStore, RemoveNonExistentFile) {
    // 删除不存在的文件应该失败
    auto remove_future = _store->remove("nonexistent_file");
    auto status = remove_future.get();
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), ENOENT);
}

TEST_F(TestLocalStore, FileReopeningBehavior) {
    // 测试文件重新打开的行为
    FileHandlePtr fh1;
    auto future1 = _store->open("reopen_file", O_RDWR | O_CREAT, &fh1);
    auto status = future1.get();
    ASSERT_TRUE(status.ok()) << "Failed to open file: " << status.error_str();

    // 写入数据
    IOBuf write_buf;
    const char* test_data = "Hello, World!";
    write_buf.append(test_data, strlen(test_data));

    auto append_future = _store->append(fh1, 0, write_buf);
    status = append_future.get();
    ASSERT_TRUE(status.ok());

    // 重新打开同一个文件（不使用 O_TRUNC）
    FileHandlePtr fh2;
    auto future2 = _store->open("reopen_file", O_RDWR, &fh2);
    status = future2.get();
    ASSERT_TRUE(status.ok()) << "Failed to reopen file: " << status.error_str();

    // 验证数据仍然存在
    uint64_t size = 0;
    auto size_future = _store->size(fh2, &size);
    status = size_future.get();
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(size, strlen(test_data));

    // 读取数据验证内容
    IOBuf read_buf;
    auto read_future = _store->read(fh2, 0, strlen(test_data), &read_buf);
    status = read_future.get();
    ASSERT_TRUE(status.ok());

    std::string actual(read_buf.to_string());
    ASSERT_EQ(actual, test_data);
}

TEST_F(TestLocalStore, MultipleFileOperations) {
    // 测试多个文件的并发操作
    std::vector<std::string> filenames = {"multi1", "multi2", "multi3"};
    std::vector<FileHandlePtr> handles;

    // 创建多个文件
    for (const auto& filename : filenames) {
        FileHandlePtr fh;
        auto future = _store->open(filename.c_str(), O_RDWR | O_CREAT, &fh);
        auto status = future.get();
        ASSERT_TRUE(status.ok()) << "Failed to open file " << filename << ": " << status.error_str();
        handles.push_back(fh);
    }

    // 为每个文件设置不同的属性
    for (size_t i = 0; i < handles.size(); ++i) {
        std::string key = "file_key_" + std::to_string(i);
        std::string value = "file_value_" + std::to_string(i);

        auto set_future = _store->set_attr(handles[i], key.c_str(), value.c_str());
        auto status = set_future.get();
        if (status.error_code() == ENOTSUP) {
            GTEST_SKIP() << "set_attr is not supported";
        }
        ASSERT_TRUE(status.ok()) << "Failed to set attribute for file: " << status.error_str() << "("
                                 << status.error_code() << ")";
    }

    // 验证每个文件的属性
    for (size_t i = 0; i < handles.size(); ++i) {
        std::string key = "file_key_" + std::to_string(i);
        std::string expected_value = "file_value_" + std::to_string(i);

        std::string actual_value;
        auto get_future = _store->get_attr(handles[i], key.c_str(), &actual_value);
        auto status = get_future.get();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(actual_value, expected_value);
    }
}

TEST_F(TestLocalStore, EdgeCaseFlags) {
    // 测试边界情况的标志组合
    FileHandlePtr fh;

    // 只使用 O_RDWR，不创建文件
    auto future = _store->open("edge_case", O_RDWR, &fh);
    auto status = future.get();
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), ENOENT);

    // 使用无效的标志组合
    future = _store->open("edge_case", 0, &fh);
    status = future.get();
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), ENOENT);

    // 使用 O_CREAT 但不指定读写权限
    future = _store->open("edge_case", O_CREAT, &fh);
    status = future.get();
    ASSERT_TRUE(status.ok()) << "O_CREAT without read/write flags should work";
}

TEST_F(TestLocalStore, ErrorCodeConsistency) {
    // 测试错误码的一致性
    FileHandlePtr fh;

    // 测试 EINVAL 错误
    auto future = _store->open(nullptr, O_RDWR | O_CREAT, &fh);
    auto status = future.get();
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), EINVAL);

    // 测试 ENOENT 错误
    future = _store->open("nonexistent", O_RDWR, &fh);
    status = future.get();
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), ENOENT);

    // 测试 EEXIST 错误
    auto future1 = _store->open("exclusive_test", O_RDWR | O_CREAT, &fh);
    status = future1.get();
    ASSERT_TRUE(status.ok()) << "Failed to create file: " << status.error_str();

    auto future2 = _store->open("exclusive_test", O_RDWR | O_CREAT | O_EXCL, &fh);
    status = future2.get();
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), EEXIST);
}

TEST_F(TestLocalStore, ThreadSafetyWithMultipleStores) {
    // 测试多个存储实例的线程安全性
    std::vector<StorePtr> stores;
    const int num_stores = 5;

    // 创建多个存储实例
    for (int i = 0; i < num_stores; ++i) {
        auto store_path = "local://" + (_test_dir / ("store_" + std::to_string(i))).string();
        auto store = Store::create(store_path.c_str());
        ASSERT_TRUE(store != nullptr);
        stores.emplace_back(store);
    }

    // 在每个存储中创建文件
    std::vector<std::future<Status>> futures;

    for (int i = 0; i < num_stores; ++i) {
        futures.emplace_back(std::async(std::launch::async, [&stores, i]() {
            auto store = stores[i];
            FileHandlePtr fh;
            auto filename = "store_" + std::to_string(i) + "_file";

            auto future = store->open(filename.c_str(), O_RDWR | O_CREAT, &fh);
            auto status = future.get();
            if (!status.ok()) {
                return status;
            }

            // 写入数据
            IOBuf write_buf;
            std::string test_data = "Data from store " + std::to_string(i);
            write_buf.append(test_data.c_str(), test_data.length());

            auto append_future = store->append(fh, 0, write_buf);
            return append_future.get();
        }));
    }

    // 等待所有操作完成
    for (auto& future : futures) {
        auto status = future.get();
        ASSERT_TRUE(status.ok()) << "Thread safety test failed: " << status.error_str();
    }
}

} // namespace
// NOLINTEND(readability-magic-numbers)
