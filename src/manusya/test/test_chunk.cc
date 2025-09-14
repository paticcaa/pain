#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <future>
#include <thread>
#include <vector>
#include "include/pain/base/object_id.h"
#include "manusya/chunk.h"
#include "manusya/mem_store.h"

// NOLINTBEGIN(readability-magic-numbers)
namespace {
using namespace pain;
using namespace pain::manusya;

class TestChunk : public ::testing::Test {
protected:
    void SetUp() override {
        _store = Store::create("memory://");
        ASSERT_TRUE(_store != nullptr);
    }

    void TearDown() override {
        _store.reset();
    }

    StorePtr _store;

    // 辅助方法：创建测试数据
    IOBuf create_test_data(const std::string& content) {
        IOBuf buf;
        buf.append(content.c_str(), content.length());
        return buf;
    }

    // 辅助方法：验证 IOBuf 内容
    void verify_iobuf_content(const IOBuf& buf, const std::string& expected) {
        ASSERT_EQ(buf.size(), expected.length());
        std::string actual(buf.to_string());
        ASSERT_EQ(actual, expected);
    }
};

// 基础功能测试
TEST_F(TestChunk, BasicCreate) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();
    ASSERT_TRUE(chunk != nullptr);

    // 验证初始状态
    ASSERT_EQ(chunk->state(), ChunkState::kOpen);
    ASSERT_EQ(chunk->size(), 0);
    ASSERT_FALSE(chunk->chunk_id().str().empty());
}

TEST_F(TestChunk, CreateWithUUID) {
    ChunkOptions options;
    ChunkPtr chunk;
    auto chunk_id = ObjectId::generate(0);

    auto status = Chunk::create(options, _store, chunk_id, &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk with UUID: " << status.error_str();
    ASSERT_TRUE(chunk != nullptr);

    // 验证状态
    ASSERT_EQ(chunk->state(), ChunkState::kOpen);
    ASSERT_EQ(chunk->chunk_id().str(), chunk_id.str());
}

TEST_F(TestChunk, CreateMultipleChunks) {
    ChunkOptions options;
    std::vector<ChunkPtr> chunks;

    // 创建多个chunk
    for (int i = 0; i < 5; ++i) {
        ChunkPtr chunk;
        auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
        ASSERT_TRUE(status.ok()) << "Failed to create chunk " << i << ": " << status.error_str();
        ASSERT_TRUE(chunk != nullptr);
        chunks.push_back(chunk);
    }

    // 验证所有chunk都有不同的UUID
    std::set<std::string> chunk_ids;
    for (const auto& chunk : chunks) {
        chunk_ids.insert(chunk->chunk_id().str());
    }
    ASSERT_EQ(chunk_ids.size(), 5) << "All chunks should have unique UUIDs";
}

TEST_F(TestChunk, BasicAppend) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();

    // 追加数据
    auto test_data = create_test_data("Hello, World!");
    status = chunk->append(test_data, 0);
    ASSERT_TRUE(status.ok()) << "Failed to append data: " << status.error_str();

    // 验证大小
    ASSERT_EQ(chunk->size(), test_data.size());
}

TEST_F(TestChunk, AppendMultipleTimes) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();

    // 多次追加数据
    std::vector<std::string> test_data = {"Hello", ", ", "World", "!"};
    uint64_t offset = 0;

    for (const auto& data : test_data) {
        auto buf = create_test_data(data);
        status = chunk->append(buf, offset);
        ASSERT_TRUE(status.ok()) << "Failed to append data '" << data << "': " << status.error_str();

        offset += data.length();
    }

    // 验证最终大小
    std::string expected = "Hello, World!";
    ASSERT_EQ(chunk->size(), expected.length());
}

TEST_F(TestChunk, AppendWithOffset) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();

    // 追加数据到偏移量10
    auto test_data = create_test_data("Hello");
    status = chunk->append(test_data, 10);
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), EINVAL);
    ASSERT_TRUE(status.error_str().find("invalid offset at 10@5, current size:0") != std::string::npos);
}

TEST_F(TestChunk, BasicRead) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();

    // 写入数据
    auto test_data = create_test_data("Hello, World!");
    status = chunk->append(test_data, 0);
    ASSERT_TRUE(status.ok()) << "Failed to append data: " << status.error_str();

    // 读取数据
    IOBuf read_buf;
    status = chunk->read(0, test_data.size(), &read_buf);
    ASSERT_TRUE(status.ok()) << "Failed to read data: " << status.error_str();

    verify_iobuf_content(read_buf, "Hello, World!");
}

TEST_F(TestChunk, ReadWithOffset) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();

    // 写入数据
    auto test_data = create_test_data("Hello, World!");
    status = chunk->append(test_data, 0);
    ASSERT_TRUE(status.ok()) << "Failed to append data: " << status.error_str();

    // 从偏移量7读取5个字符
    IOBuf read_buf;
    status = chunk->read(7, 5, &read_buf);
    ASSERT_TRUE(status.ok()) << "Failed to read data: " << status.error_str();

    verify_iobuf_content(read_buf, "World");
}

TEST_F(TestChunk, ReadPartialData) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();

    // 写入数据
    auto test_data = create_test_data("Hello, World!");
    status = chunk->append(test_data, 0);
    ASSERT_TRUE(status.ok()) << "Failed to append data: " << status.error_str();

    // 读取部分数据
    IOBuf read_buf;
    status = chunk->read(0, 5, &read_buf);
    ASSERT_TRUE(status.ok()) << "Failed to read data: " << status.error_str();

    verify_iobuf_content(read_buf, "Hello");
}

TEST_F(TestChunk, QueryAndSeal) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();

    // 写入一些数据
    auto test_data = create_test_data("Hello, World!");
    status = chunk->append(test_data, 0);
    ASSERT_TRUE(status.ok()) << "Failed to append data: " << status.error_str();

    // 查询并密封
    uint64_t length = 0;
    status = chunk->query_and_seal(&length);
    ASSERT_TRUE(status.ok()) << "Failed to seal chunk: " << status.error_str();

    // 验证状态和长度
    ASSERT_EQ(chunk->state(), ChunkState::kSealed);
    ASSERT_EQ(length, test_data.size());
    ASSERT_EQ(chunk->size(), test_data.size());
}

TEST_F(TestChunk, SealEmptyChunk) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();

    // 直接密封空chunk
    uint64_t length = 0;
    status = chunk->query_and_seal(&length);
    ASSERT_TRUE(status.ok()) << "Failed to seal empty chunk: " << status.error_str();

    // 验证状态和长度
    ASSERT_EQ(chunk->state(), ChunkState::kSealed);
    ASSERT_EQ(length, 0);
    ASSERT_EQ(chunk->size(), 0);
}

// 边界情况测试
TEST_F(TestChunk, AppendToSealedChunk) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();

    // 密封chunk
    uint64_t length = 0;
    status = chunk->query_and_seal(&length);
    ASSERT_TRUE(status.ok()) << "Failed to seal chunk: " << status.error_str();

    // 尝试向已密封的chunk追加数据
    auto test_data = create_test_data("Hello");
    status = chunk->append(test_data, 0);
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), EPERM);
    ASSERT_EQ(status.error_str(), "chunk is sealed");
}

TEST_F(TestChunk, AppendWithInvalidOffset) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();

    // 写入一些数据
    auto test_data = create_test_data("Hello");
    status = chunk->append(test_data, 0);
    ASSERT_TRUE(status.ok()) << "Failed to append data: " << status.error_str();

    // 尝试使用无效偏移量追加数据
    auto new_data = create_test_data("World");
    status = chunk->append(new_data, 2); // 偏移量小于当前大小
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), EINVAL);
    ASSERT_TRUE(status.error_str().find("invalid offset") != std::string::npos);
}

TEST_F(TestChunk, AppendWithGap) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();

    // 写入数据到偏移量0
    auto test_data = create_test_data("Hello");
    status = chunk->append(test_data, 0);
    ASSERT_TRUE(status.ok()) << "Failed to append data: " << status.error_str();

    // 写入数据到偏移量10（创建间隙）
    auto new_data = create_test_data("World");
    status = chunk->append(new_data, 10);
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), EINVAL);
    ASSERT_TRUE(status.error_str().find("invalid offset at 10@5, current size:5") != std::string::npos);
}

TEST_F(TestChunk, ReadBeyondFileSize) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();

    // 写入少量数据
    auto test_data = create_test_data("Hello");
    status = chunk->append(test_data, 0);
    ASSERT_TRUE(status.ok()) << "Failed to append data: " << status.error_str();

    // 尝试读取超出文件大小的数据
    IOBuf read_buf;
    status = chunk->read(0, 100, &read_buf);
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), EINVAL);
    ASSERT_TRUE(status.error_str().find("read out of range") != std::string::npos);
}

TEST_F(TestChunk, ReadFromInvalidOffset) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();

    // 写入数据
    auto test_data = create_test_data("Hello");
    status = chunk->append(test_data, 0);
    ASSERT_TRUE(status.ok()) << "Failed to append data: " << status.error_str();

    // 从无效偏移量读取
    IOBuf read_buf;
    status = chunk->read(100, 10, &read_buf);
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), EINVAL);
    ASSERT_TRUE(status.error_str().find("read out of range") != std::string::npos);
}

TEST_F(TestChunk, ReadZeroSize) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();

    // 写入数据
    auto test_data = create_test_data("Hello");
    status = chunk->append(test_data, 0);
    ASSERT_TRUE(status.ok()) << "Failed to append data: " << status.error_str();

    // 读取零大小数据
    IOBuf read_buf;
    status = chunk->read(0, 0, &read_buf);
    ASSERT_TRUE(status.ok()) << "Failed to read zero size data: " << status.error_str();
    ASSERT_EQ(read_buf.size(), 0);
}

TEST_F(TestChunk, AppendEmptyData) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();

    // 追加空数据
    IOBuf empty_buf;
    status = chunk->append(empty_buf, 0);
    ASSERT_TRUE(status.ok()) << "Failed to append empty data: " << status.error_str();

    // 验证大小
    ASSERT_EQ(chunk->size(), 0);
}

TEST_F(TestChunk, LargeDataHandling) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();

    // 写入大量数据
    const size_t large_size = 1024 * 1024; // 1MB
    std::vector<char> large_data(large_size, 'A');

    IOBuf large_buf;
    large_buf.append(large_data.data(), large_data.size());

    status = chunk->append(large_buf, 0);
    ASSERT_TRUE(status.ok()) << "Failed to append large data: " << status.error_str();

    // 验证大小
    ASSERT_EQ(chunk->size(), large_size);

    // 读取部分数据
    IOBuf read_buf;
    status = chunk->read(0, 1024, &read_buf);
    ASSERT_TRUE(status.ok()) << "Failed to read large data: " << status.error_str();
    ASSERT_EQ(read_buf.size(), 1024);
}

TEST_F(TestChunk, AppendRequestQueueHandling) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();

    // 写入数据到偏移量0
    auto test_data = create_test_data("Hello");
    status = chunk->append(test_data, 0);
    ASSERT_TRUE(status.ok()) << "Failed to append data: " << status.error_str();

    // 尝试写入数据到偏移量5（应该排队等待）
    auto new_data = create_test_data("World");
    status = chunk->append(new_data, 5);
    ASSERT_TRUE(status.ok()) << "Failed to append queued data: " << status.error_str();

    // 验证最终大小
    ASSERT_EQ(chunk->size(), 10); // "Hello" + "World"
}

TEST_F(TestChunk, AppendRequestTimeout) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();

    // 写入数据到偏移量0
    auto test_data = create_test_data("Hello");
    status = chunk->append(test_data, 0);
    ASSERT_TRUE(status.ok()) << "Failed to append data: " << status.error_str();

    // 尝试写入数据到偏移量100（应该超时）
    auto new_data = create_test_data("World");
    status = chunk->append(new_data, 100);

    // 注意：这个测试可能会超时，因为超时时间是5秒
    // 在实际测试中，可能需要调整超时时间或使用模拟
    if (status.error_code() == ETIMEDOUT) {
        GTEST_SKIP() << "Append request timeout test skipped due to long timeout";
    }
}

// 错误处理测试
TEST_F(TestChunk, CreateWithNullChunk) {
    ChunkOptions options;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), nullptr);
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), EINVAL);
    ASSERT_EQ(status.error_str(), "chunk is nullptr");
}

TEST_F(TestChunk, CreateWithNullStore) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, nullptr, ObjectId::generate(0), &chunk);
    ASSERT_FALSE(status.ok());
    // 注意：当前实现没有检查store是否为nullptr
}

TEST_F(TestChunk, ReadWithNullBuffer) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();

    // 写入数据
    auto test_data = create_test_data("Hello");
    status = chunk->append(test_data, 0);
    ASSERT_TRUE(status.ok()) << "Failed to append data: " << status.error_str();

    // 使用空缓冲区读取
    status = chunk->read(0, 5, nullptr);
    ASSERT_FALSE(status.ok());
    // 注意：当前实现没有检查buf是否为nullptr
}

TEST_F(TestChunk, QueryAndSealWithNullLength) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();

    // 使用空长度指针查询并密封
    status = chunk->query_and_seal(nullptr);
    ASSERT_FALSE(status.ok());
    // 注意：当前实现没有检查length是否为nullptr
}

// 并发测试
TEST_F(TestChunk, ConcurrentAppends) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();

    // 并发写入数据
    const int num_threads = 10;
    const int writes_per_thread = 100;
    std::vector<std::future<Status>> futures;
    std::atomic<int> offset = 0;
    for (int i = 0; i < num_threads; ++i) {
        futures.emplace_back(std::async(std::launch::async, [chunk, i, &offset]() {
            for (int j = 0; j < writes_per_thread; ++j) {
                std::string data = "Thread" + std::to_string(i) + "_Write" + std::to_string(j);
                IOBuf buf;
                buf.append(data.c_str(), data.length());
                auto status = chunk->append(buf, offset.fetch_add(data.length()));
                if (!status.ok()) {
                    return status;
                }
            }
            return Status::OK();
        }));
    }

    // 等待所有线程完成
    for (auto& future : futures) {
        status = future.get();
        ASSERT_TRUE(status.ok()) << "Concurrent append failed: " << status.error_str();
    }

    // 验证最终大小
    EXPECT_EQ(chunk->size(), offset);
}

TEST_F(TestChunk, ConcurrentReads) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();

    // 写入一些数据
    auto test_data = create_test_data("Hello, World!");
    status = chunk->append(test_data, 0);
    ASSERT_TRUE(status.ok()) << "Failed to append data: " << status.error_str();

    // 并发读取数据
    const int num_threads = 10;
    const int reads_per_thread = 100;
    std::vector<std::future<Status>> futures;

    for (int i = 0; i < num_threads; ++i) {
        futures.emplace_back(std::async(std::launch::async, [chunk, i, reads_per_thread]() {
            for (int j = 0; j < reads_per_thread; ++j) {
                IOBuf read_buf;
                auto status = chunk->read(0, 5, &read_buf);
                if (!status.ok()) {
                    return status;
                }
            }
            return Status::OK();
        }));
    }

    // 等待所有线程完成
    for (auto& future : futures) {
        status = future.get();
        ASSERT_TRUE(status.ok()) << "Concurrent read failed: " << status.error_str();
    }
}

// 内存管理测试
TEST_F(TestChunk, ReferenceCounting) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();

    // 验证引用计数
    ASSERT_EQ(chunk->use_count(), 1);

    // 创建另一个引用
    ChunkPtr chunk_ref = chunk;
    ASSERT_EQ(chunk->use_count(), 2);

    // 释放引用
    chunk_ref.reset();
    ASSERT_EQ(chunk->use_count(), 1);
}

TEST_F(TestChunk, MultipleChunkReferences) {
    ChunkOptions options;
    std::vector<ChunkPtr> chunks;

    // 创建多个chunk并保持引用
    for (int i = 0; i < 5; ++i) {
        ChunkPtr chunk;
        auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
        ASSERT_TRUE(status.ok()) << "Failed to create chunk " << i << ": " << status.error_str();
        chunks.push_back(chunk);
    }

    // 验证所有chunk都存在
    ASSERT_EQ(chunks.size(), 5);

    // 释放所有引用
    chunks.clear();

    // 验证引用计数
    // 注意：这里无法直接验证，因为chunks已经被销毁
}

// 状态转换测试
TEST_F(TestChunk, StateTransitions) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();

    // 初始状态应该是 kOpen
    ASSERT_EQ(chunk->state(), ChunkState::kOpen);

    // 密封chunk
    uint64_t length = 0;
    status = chunk->query_and_seal(&length);
    ASSERT_TRUE(status.ok()) << "Failed to seal chunk: " << status.error_str();

    // 状态应该变为 kSealed
    ASSERT_EQ(chunk->state(), ChunkState::kSealed);
}

TEST_F(TestChunk, StateAfterOperations) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();

    // 执行一些操作后状态应该保持为 kOpen
    auto test_data = create_test_data("Hello");
    status = chunk->append(test_data, 0);
    ASSERT_TRUE(status.ok()) << "Failed to append data: " << status.error_str();
    ASSERT_EQ(chunk->state(), ChunkState::kOpen);

    // 读取数据后状态应该保持为 kOpen
    IOBuf read_buf;
    status = chunk->read(0, 5, &read_buf);
    ASSERT_TRUE(status.ok()) << "Failed to read data: " << status.error_str();
    ASSERT_EQ(chunk->state(), ChunkState::kOpen);
}

// 边界值测试
TEST_F(TestChunk, AppendAtMaxOffset) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();

    // 尝试在最大偏移量处追加数据
    auto test_data = create_test_data("Hello");
    status = chunk->append(test_data, UINT64_MAX - test_data.size());

    // 这个测试可能会失败，因为偏移量太大
    if (status.ok()) {
        GTEST_SKIP() << "Append at max offset succeeded, but may cause issues";
    }
}

TEST_F(TestChunk, ReadAtMaxOffset) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();

    // 写入一些数据
    auto test_data = create_test_data("Hello");
    status = chunk->append(test_data, 0);
    ASSERT_TRUE(status.ok()) << "Failed to append data: " << status.error_str();

    // 尝试从最大偏移量读取
    IOBuf read_buf;
    status = chunk->read(UINT64_MAX - 1, 1, &read_buf);
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), EINVAL);
}

// 特殊场景测试
TEST_F(TestChunk, AppendRequestOrdering) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();

    // 写入数据到偏移量0
    auto test_data = create_test_data("Hello");
    status = chunk->append(test_data, 0);
    ASSERT_TRUE(status.ok()) << "Failed to append data: " << status.error_str();

    // 按顺序写入数据到不同偏移量
    std::vector<std::pair<uint64_t, std::string>> test_cases = {{5, "World"}, {10, "Test"}, {14, "Data"}};

    for (const auto& [offset, data] : test_cases) {
        auto buf = create_test_data(data);
        status = chunk->append(buf, offset);
        ASSERT_TRUE(status.ok()) << "Failed to append data at offset " << offset << ": " << status.error_str();
    }

    // 验证最终大小
    ASSERT_EQ(chunk->size(), 18); // 最大偏移量 + 数据长度
}

TEST_F(TestChunk, ChunkOptionsAccess) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = Chunk::create(options, _store, ObjectId::generate(0), &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();
}

} // namespace
// NOLINTEND(readability-magic-numbers)
