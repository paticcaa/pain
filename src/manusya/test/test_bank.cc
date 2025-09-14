#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <set>
#include <thread>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include "manusya/bank.h"
#include "manusya/mem_store.h"

// NOLINTBEGIN(readability-magic-numbers)
namespace {
using namespace pain;
using namespace pain::manusya;

class TestBank : public ::testing::Test {
protected:
    void SetUp() override {
        _store = Store::create("memory://");
        ASSERT_TRUE(_store != nullptr);
        _bank = std::make_unique<Bank>(_store);
    }

    void TearDown() override {
        _bank.reset();
        _store.reset();
    }

    StorePtr _store;
    std::unique_ptr<Bank> _bank;
};

TEST_F(TestBank, BasicCreateChunk) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = _bank->create_chunk(options, 0, &chunk);
    ASSERT_TRUE(status.ok()) << "Failed to create chunk: " << status.error_str();
    ASSERT_TRUE(chunk != nullptr);
    ASSERT_EQ(chunk->state(), ChunkState::kOpen);
    ASSERT_EQ(chunk->size(), 0);
}

TEST_F(TestBank, CreateMultipleChunks) {
    ChunkOptions options;
    std::vector<ChunkPtr> chunks;

    // 创建多个chunk
    for (int i = 0; i < 5; ++i) {
        ChunkPtr chunk;
        auto status = _bank->create_chunk(options, i, &chunk);
        ASSERT_TRUE(status.ok()) << "Failed to create chunk " << i << ": " << status.error_str();
        ASSERT_TRUE(chunk != nullptr);
        chunks.push_back(chunk);
    }

    // 验证所有chunk都有不同的UUID
    std::set<std::string> chunk_ids;
    std::vector<std::string> chunk_ids_str;
    for (const auto& chunk : chunks) {
        chunk_ids.insert(chunk->chunk_id().str());
        chunk_ids_str.push_back(chunk->chunk_id().str());
    }
    ASSERT_EQ(chunk_ids.size(), 5) << "All chunks should have unique UUIDs:" << fmt::format("{}", chunk_ids_str);
}

TEST_F(TestBank, GetChunkSuccess) {
    ChunkOptions options;
    ChunkPtr created_chunk;

    auto status = _bank->create_chunk(options, 0, &created_chunk);
    ASSERT_TRUE(status.ok());

    ChunkPtr retrieved_chunk;
    status = _bank->get_chunk(created_chunk->chunk_id(), &retrieved_chunk);
    ASSERT_TRUE(status.ok()) << "Failed to get chunk: " << status.error_str();
    ASSERT_TRUE(retrieved_chunk != nullptr);
    ASSERT_EQ(retrieved_chunk->chunk_id().str(), created_chunk->chunk_id().str());
    ASSERT_EQ(retrieved_chunk->state(), created_chunk->state());
}

TEST_F(TestBank, GetChunkNotFound) {
    auto non_existent_chunk_id = ObjectId::generate(0);
    ChunkPtr chunk;

    auto status = _bank->get_chunk(non_existent_chunk_id, &chunk);
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), ENOENT);
    ASSERT_EQ(status.error_str(), "Chunk not found");
}

TEST_F(TestBank, RemoveChunkSuccess) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = _bank->create_chunk(options, 0, &chunk);
    ASSERT_TRUE(status.ok());

    auto chunk_id = chunk->chunk_id();
    status = _bank->remove_chunk(chunk_id);
    ASSERT_TRUE(status.ok()) << "Failed to remove chunk: " << status.error_str();

    // 验证chunk已被删除
    ChunkPtr retrieved_chunk;
    status = _bank->get_chunk(chunk_id, &retrieved_chunk);
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), ENOENT);
}

TEST_F(TestBank, RemoveChunkNotFound) {
    auto non_existent_chunk_id = ObjectId::generate(0);

    auto status = _bank->remove_chunk(non_existent_chunk_id);
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), ENOENT);
    ASSERT_EQ(status.error_str(), "Chunk not found");
}

TEST_F(TestBank, ListChunkEmpty) {
    std::vector<ObjectId> listed_chunk_ids;
    auto start_chunk_id = ObjectId::generate(0);

    _bank->list_chunk(start_chunk_id, 10, [&listed_chunk_ids](ObjectId chunk_id) {
        listed_chunk_ids.push_back(chunk_id);
    });

    ASSERT_TRUE(listed_chunk_ids.empty()) << "Should not list any chunks when bank is empty";
}

TEST_F(TestBank, ListChunkWithLimit) {
    ChunkOptions options;
    std::vector<ObjectId> created_chunk_ids;

    // 创建10个chunk
    for (int i = 0; i < 10; ++i) {
        ChunkPtr chunk;
        auto status = _bank->create_chunk(options, i, &chunk);
        ASSERT_TRUE(status.ok());
        created_chunk_ids.push_back(chunk->chunk_id());
    }

    // 测试限制数量为5
    std::vector<ObjectId> listed_chunk_ids;
    ObjectId start_chunk_id = ObjectId::generate(0);
    _bank->list_chunk(start_chunk_id, 5, [&listed_chunk_ids](ObjectId chunk_id) {
        listed_chunk_ids.push_back(chunk_id);
    });

    ASSERT_EQ(listed_chunk_ids.size(), 5) << "Should list exactly 5 chunks";
}

TEST_F(TestBank, ListChunkWithStartUUID) {
    ChunkOptions options;
    std::vector<ObjectId> created_chunk_ids;

    // 创建5个chunk
    for (int i = 0; i < 5; ++i) {
        ChunkPtr chunk;
        auto status = _bank->create_chunk(options, i, &chunk);
        ASSERT_TRUE(status.ok());
        created_chunk_ids.push_back(chunk->chunk_id());
    }

    // 排序UUID以便测试
    std::sort(created_chunk_ids.begin(), created_chunk_ids.end());

    // 从第二个UUID开始列出
    std::vector<ObjectId> listed_chunk_ids;
    _bank->list_chunk(created_chunk_ids[1], 10, [&listed_chunk_ids](ObjectId chunk_id) {
        listed_chunk_ids.push_back(chunk_id);
    });

    ASSERT_EQ(listed_chunk_ids.size(), 4) << "Should list 4 chunks starting from second UUID";
    ASSERT_EQ(listed_chunk_ids[0].str(), created_chunk_ids[1].str()) << "First listed UUID should match start UUID";
}

TEST_F(TestBank, LoadEmptyStore) {
    auto status = _bank->load();
    ASSERT_TRUE(status.ok()) << "Load should succeed even with empty store";
}

TEST_F(TestBank, ConcurrentOperations) {
    ChunkOptions options;
    const int num_threads = 10;
    const int chunks_per_thread = 5;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    // 创建多个线程同时操作bank
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([this, &options, &success_count, i]() {
            for (int j = 0; j < chunks_per_thread; ++j) {
                ChunkPtr chunk;
                auto status = _bank->create_chunk(options, i, &chunk);
                if (status.ok()) {
                    success_count++;

                    // 尝试获取刚创建的chunk
                    ChunkPtr retrieved_chunk;
                    status = _bank->get_chunk(chunk->chunk_id(), &retrieved_chunk);
                    if (status.ok()) {
                        ASSERT_EQ(retrieved_chunk->chunk_id().str(), chunk->chunk_id().str());
                    }
                }
            }
        });
    }

    // 等待所有线程完成
    for (auto& thread : threads) {
        thread.join();
    }

    ASSERT_EQ(success_count.load(), num_threads * chunks_per_thread) << "All chunk creations should succeed";
}

TEST_F(TestBank, ChunkStateTransitions) {
    ChunkOptions options;
    ChunkPtr chunk;

    auto status = _bank->create_chunk(options, 0, &chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(chunk->state(), ChunkState::kOpen);

    // 测试chunk的append操作
    IOBuf test_data;
    test_data.append("test data");
    status = chunk->append(test_data, 0);
    ASSERT_TRUE(status.ok()) << "Failed to append data: " << status.error_str();

    // 验证size已更新
    ASSERT_GT(chunk->size(), 0);
}

TEST_F(TestBank, SingletonInstance) {
    auto& instance1 = Bank::instance();
    auto& instance2 = Bank::instance();

    ASSERT_EQ(&instance1, &instance2) << "Bank::instance() should return the same instance";
}

TEST_F(TestBank, StoreIntegration) {
    ChunkOptions options;
    ChunkPtr chunk;

    // 创建chunk
    auto status = _bank->create_chunk(options, 0, &chunk);
    ASSERT_TRUE(status.ok());

    // 验证chunk被正确存储
    ChunkPtr retrieved_chunk;
    status = _bank->get_chunk(chunk->chunk_id(), &retrieved_chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(retrieved_chunk->chunk_id().str(), chunk->chunk_id().str());

    // 删除chunk
    status = _bank->remove_chunk(chunk->chunk_id());
    ASSERT_TRUE(status.ok());

    // 验证chunk已被删除
    status = _bank->get_chunk(chunk->chunk_id(), &retrieved_chunk);
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), ENOENT);
}

TEST_F(TestBank, ErrorHandling) {
    // 测试空指针参数
    ChunkPtr* null_chunk_ptr = nullptr;
    ChunkOptions options;

    auto status = _bank->create_chunk(options, 0, null_chunk_ptr);

    // 测试无效的UUID
    auto invalid_chunk_id = ObjectId::generate(0);
    ChunkPtr chunk;
    status = _bank->get_chunk(invalid_chunk_id, &chunk);
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.error_code(), ENOENT);
}

} // namespace
// NOLINTEND(readability-magic-numbers)
