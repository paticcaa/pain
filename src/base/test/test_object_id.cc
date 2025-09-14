#include <gtest/gtest.h>
#include <pain/base/object_id.h>
#include <pain/base/uuid.h>
#include <algorithm>
#include <string>
#include <vector>

// NOLINTBEGIN
namespace {
using namespace pain;

class TestObjectId : public ::testing::Test {
protected:
    void SetUp() override {
        // 创建一些测试用的 UUID
        test_uuid1 = UUID::from_str_or_die("050e8400-0000-0000-0000-000000000000");
        test_uuid2 = UUID::from_str_or_die("6ba7b810-9dad-11d1-80b4-80c04fd430c8");
        test_uuid3 = UUID::from_str_or_die("6ba7b811-9dad-11d1-90b4-90c04fd430c8");
        ASSERT_LT(test_uuid1, test_uuid2) << fmt::format(
            "uuid1:{},{} uuid2:{},{}", test_uuid1.low(), test_uuid1.high(), test_uuid2.low(), test_uuid2.high());
        ASSERT_LT(test_uuid2, test_uuid3) << fmt::format(
            "uuid2:{},{} uuid3:{},{}", test_uuid2.low(), test_uuid2.high(), test_uuid3.low(), test_uuid3.high());
    }

    UUID test_uuid1;
    UUID test_uuid2;
    UUID test_uuid3;
};

// 构造函数和访问器测试
TEST_F(TestObjectId, ConstructorAndAccessors) {
    ObjectId obj(123, test_uuid1);

    EXPECT_EQ(obj.partition_id(), 123);
    EXPECT_EQ(obj.uuid(), test_uuid1);
}

TEST_F(TestObjectId, ConstructorWithZeroPartitionId) {
    ObjectId obj(0, test_uuid1);

    EXPECT_EQ(obj.partition_id(), 0);
    EXPECT_EQ(obj.uuid(), test_uuid1);
}

TEST_F(TestObjectId, ConstructorWithMaxPartitionId) {
    ObjectId obj(UINT32_MAX, test_uuid1);

    EXPECT_EQ(obj.partition_id(), UINT32_MAX);
    EXPECT_EQ(obj.uuid(), test_uuid1);
}

// 相等性操作符测试
TEST_F(TestObjectId, EqualityOperator) {
    ObjectId obj1(123, test_uuid1);
    ObjectId obj2(123, test_uuid1);
    ObjectId obj3(456, test_uuid1);
    ObjectId obj4(123, test_uuid2);

    EXPECT_TRUE(obj1 == obj2);
    EXPECT_FALSE(obj1 == obj3);
    EXPECT_FALSE(obj1 == obj4);
}

TEST_F(TestObjectId, InequalityOperator) {
    ObjectId obj1(123, test_uuid1);
    ObjectId obj2(123, test_uuid1);
    ObjectId obj3(456, test_uuid1);
    ObjectId obj4(123, test_uuid2);

    EXPECT_FALSE(obj1 != obj2);
    EXPECT_TRUE(obj1 != obj3);
    EXPECT_TRUE(obj1 != obj4);
}

// 比较操作符测试
TEST_F(TestObjectId, LessThanOperator) {
    ObjectId obj1(100, test_uuid1);
    ObjectId obj2(200, test_uuid1);
    ObjectId obj3(100, test_uuid2);
    ObjectId obj4(100, test_uuid1);

    // 不同 partition_id
    EXPECT_TRUE(obj1 < obj2);
    EXPECT_FALSE(obj2 < obj1);

    // 相同 partition_id，不同 UUID
    EXPECT_TRUE(obj1 < obj3);
    EXPECT_FALSE(obj3 < obj1);

    // 相同 partition_id 和 UUID
    EXPECT_FALSE(obj1 < obj4);
    EXPECT_FALSE(obj4 < obj1);
}

TEST_F(TestObjectId, GreaterThanOperator) {
    ObjectId obj1(100, test_uuid1);
    ObjectId obj2(200, test_uuid1);
    ObjectId obj3(100, test_uuid2);
    ObjectId obj4(100, test_uuid1);

    // 不同 partition_id
    EXPECT_TRUE(obj2 > obj1);
    EXPECT_FALSE(obj1 > obj2);

    // 相同 partition_id，不同 UUID
    EXPECT_TRUE(obj3 > obj1);
    EXPECT_FALSE(obj1 > obj3);

    // 相同 partition_id 和 UUID
    EXPECT_FALSE(obj1 > obj4);
    EXPECT_FALSE(obj4 > obj1);
}

TEST_F(TestObjectId, LessThanOrEqualOperator) {
    ObjectId obj1(100, test_uuid1);
    ObjectId obj2(200, test_uuid1);
    ObjectId obj3(100, test_uuid2);
    ObjectId obj4(100, test_uuid1);

    // 不同 partition_id
    EXPECT_TRUE(obj1 <= obj2);
    EXPECT_FALSE(obj2 <= obj1);

    // 相同 partition_id，不同 UUID
    EXPECT_TRUE(obj1 <= obj3);
    EXPECT_FALSE(obj3 <= obj1);

    // 相同 partition_id 和 UUID
    EXPECT_TRUE(obj1 <= obj4);
    EXPECT_TRUE(obj4 <= obj1);
}

TEST_F(TestObjectId, GreaterThanOrEqualOperator) {
    ObjectId obj1(100, test_uuid1);
    ObjectId obj2(200, test_uuid1);
    ObjectId obj3(100, test_uuid2);
    ObjectId obj4(100, test_uuid1);

    // 不同 partition_id
    EXPECT_TRUE(obj2 >= obj1);
    EXPECT_FALSE(obj1 >= obj2);

    // 相同 partition_id，不同 UUID
    EXPECT_TRUE(obj3 >= obj1);
    EXPECT_FALSE(obj1 >= obj3);

    // 相同 partition_id 和 UUID
    EXPECT_TRUE(obj1 >= obj4);
    EXPECT_TRUE(obj4 >= obj1);
}

// 字符串转换测试
TEST_F(TestObjectId, ToString) {
    ObjectId obj(123, test_uuid1);
    std::string str = obj.to_string();

    EXPECT_EQ(str, fmt::format("123-{}", test_uuid1.str()));
}

TEST_F(TestObjectId, ToStringWithZeroPartitionId) {
    ObjectId obj(0, test_uuid1);
    std::string str = obj.to_string();

    EXPECT_EQ(str, fmt::format("0-{}", test_uuid1.str()));
}

TEST_F(TestObjectId, ToStringWithMaxPartitionId) {
    ObjectId obj(UINT32_MAX, test_uuid1);
    std::string str = obj.to_string();

    EXPECT_EQ(str, fmt::format("{}-{}", UINT32_MAX, test_uuid1.str()));
}

TEST_F(TestObjectId, FromString) {
    std::string str = fmt::format("123-{}", test_uuid1.str());
    ObjectId obj = ObjectId::from_str_or_die(str);

    EXPECT_EQ(obj.partition_id(), 123);
    EXPECT_EQ(obj.uuid(), test_uuid1);
}

TEST_F(TestObjectId, FromStringWithZeroPartitionId) {
    std::string str = fmt::format("0-{}", test_uuid1.str());
    ObjectId obj = ObjectId::from_str_or_die(str);

    EXPECT_EQ(obj.partition_id(), 0);
    EXPECT_EQ(obj.uuid(), test_uuid1);
}

TEST_F(TestObjectId, FromStringWithMaxPartitionId) {
    std::string str = fmt::format("{}-{}", UINT32_MAX, test_uuid1.str());
    ObjectId obj = ObjectId::from_str_or_die(str);

    EXPECT_EQ(obj.partition_id(), UINT32_MAX);
    EXPECT_EQ(obj.uuid(), test_uuid1);
}

// 往返转换测试
TEST_F(TestObjectId, RoundTripConversion) {
    ObjectId original(12345, test_uuid2);
    std::string str = original.to_string();
    ObjectId converted = ObjectId::from_str_or_die(str);

    EXPECT_EQ(original, converted);
    EXPECT_EQ(original.partition_id(), converted.partition_id());
    EXPECT_EQ(original.uuid(), converted.uuid());
}

TEST_F(TestObjectId, RoundTripConversionWithDifferentValues) {
    std::vector<uint32_t> partition_ids = {0, 1, 100, 1000, UINT32_MAX};
    std::vector<UUID> uuids = {test_uuid1, test_uuid2, test_uuid3};

    for (auto pid : partition_ids) {
        for (const auto& uuid : uuids) {
            ObjectId original(pid, uuid);
            std::string str = original.to_string();
            ObjectId converted = ObjectId::from_str_or_die(str);

            EXPECT_EQ(original, converted) << "Failed for partition_id=" << pid;
        }
    }
}

// 排序测试
TEST_F(TestObjectId, Sorting) {
    std::vector<ObjectId> objects = {ObjectId(200, test_uuid2),
                                     ObjectId(100, test_uuid3),
                                     ObjectId(100, test_uuid1),
                                     ObjectId(150, test_uuid1),
                                     ObjectId(100, test_uuid2)};

    std::sort(objects.begin(), objects.end());

    // 验证排序结果
    EXPECT_LT(objects[0], objects[1]);
    EXPECT_LT(objects[1], objects[2]);
    EXPECT_LT(objects[2], objects[3]);
    EXPECT_LT(objects[3], objects[4]);
}

// 边界情况测试
TEST_F(TestObjectId, EdgeCasePartitionIds) {
    std::vector<uint32_t> edge_cases = {0, 1, UINT32_MAX - 1, UINT32_MAX};

    for (auto pid : edge_cases) {
        ObjectId obj(pid, test_uuid1);
        EXPECT_EQ(obj.partition_id(), pid);

        // 测试往返转换
        std::string str = obj.to_string();
        ObjectId converted = ObjectId::from_str_or_die(str);
        EXPECT_EQ(obj, converted);
    }
}

// 错误处理测试
TEST_F(TestObjectId, FromStringInvalidFormat) {
    // 测试缺少分隔符的情况
    EXPECT_DEATH({ ObjectId::from_str_or_die("123550e8400-e29b-41d4-a716-446655440000"); }, "Invalid UUID string");

    // 测试空字符串
    EXPECT_DEATH({ ObjectId::from_str_or_die(""); }, "Invalid ObjectId string");

    // 测试只有分隔符
    EXPECT_DEATH({ ObjectId::from_str_or_die("-"); }, "Invalid ObjectId string");

    // 测试多个分隔符
    EXPECT_DEATH({ ObjectId::from_str_or_die("123-456-550e8400-e29b-41d4-a716-446655440000"); }, "Invalid UUID string");
}

TEST_F(TestObjectId, FromStringInvalidUUID) {
    // 测试无效的 UUID 格式
    EXPECT_DEATH({ ObjectId::from_str_or_die("123-invalid-uuid"); }, ".*");

    // 测试空 UUID
    EXPECT_DEATH({ ObjectId::from_str_or_die("123-"); }, ".*");
}

TEST_F(TestObjectId, FromStringInvalidPartitionId) {
    // 测试非数字 partition_id
    EXPECT_DEATH({ ObjectId::from_str_or_die("abc-550e8400-e29b-41d4-a716-446655440000"); }, ".*");

    // 测试负数 partition_id（虽然 uint64_t 不能为负，但 stoull 会处理）
    EXPECT_DEATH({ ObjectId::from_str_or_die("-123-550e8400-e29b-41d4-a716-446655440000"); }, ".*");
}

// 性能测试（基本功能验证）
TEST_F(TestObjectId, PerformanceBasic) {
    const int iterations = 1000;

    for (int i = 0; i < iterations; ++i) {
        ObjectId obj(i, test_uuid1);
        std::string str = obj.to_string();
        ObjectId converted = ObjectId::from_str_or_die(str);
        EXPECT_EQ(obj, converted);
    }
}

// 哈希兼容性测试（如果用于容器）
TEST_F(TestObjectId, HashCompatibility) {
    ObjectId obj1(123, test_uuid1);
    ObjectId obj2(123, test_uuid1);
    ObjectId obj3(456, test_uuid1);

    // 相同对象应该有相同的哈希值
    EXPECT_EQ(std::hash<ObjectId>{}(obj1), std::hash<ObjectId>{}(obj2));

    // 不同对象应该有不同的哈希值（高概率）
    EXPECT_NE(std::hash<ObjectId>{}(obj1), std::hash<ObjectId>{}(obj3));
}

} // namespace
// NOLINTEND
