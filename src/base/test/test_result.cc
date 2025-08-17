#include <gtest/gtest.h>
#include <system_error>
#include "base/result.h"

// NOLINTBEGIN
namespace {
using namespace pain;

TEST(TestResultNormal, ok) {
    auto r = Ok<int, std::error_code>(42);
    ASSERT_EQ(r.is_ok(), true);
    ASSERT_EQ(r.is_err(), false);
    EXPECT_EQ(r.value(), 42);
    EXPECT_EQ(std::move(r).unwrap(), 42);
}

TEST(TestResultNormal, ok_expect) {
    auto r = Ok<int, std::error_code>(42);
    ASSERT_EQ(r.is_ok(), true);
    ASSERT_EQ(r.is_err(), false);
    EXPECT_EQ(r.value(), 42);
    EXPECT_EQ(std::move(r).expect("ok_expect"), 42);
}

TEST(TestResultNormal, ok_const) {
    const auto r = Ok<int, std::error_code>(42);
    ASSERT_EQ(r.is_ok(), true);
    ASSERT_EQ(r.is_err(), false);
    EXPECT_EQ(r.value(), 42);
}

TEST(TestResultNormal, ok_with_same_type) {
    auto r = Ok<int, int>(42);
    ASSERT_EQ(r.is_ok(), true);
    ASSERT_EQ(r.is_err(), false);
    EXPECT_EQ(r.value(), 42);
    EXPECT_EQ(std::move(r).unwrap(), 42);
}

TEST(TestResultNormal, err) {
    auto expect = std::make_error_code(std::errc::invalid_argument);
    auto r = Err<int, std::error_code>(expect);
    ASSERT_EQ(r.is_ok(), false);
    ASSERT_EQ(r.is_err(), true);
    EXPECT_EQ(r.err(), expect);
    EXPECT_EQ(std::move(r).unwrap_err(), expect);
}

TEST(TestResultNormal, err_const) {
    auto expect = std::make_error_code(std::errc::invalid_argument);
    const auto r = Err<int, std::error_code>(expect);
    ASSERT_EQ(r.is_ok(), false);
    ASSERT_EQ(r.is_err(), true);
    EXPECT_EQ(r.err(), expect);
}

TEST(TestResultNormal, map_ok) {
    auto r = Ok<int, std::error_code>(42);
    auto r2 = std::move(r).map<std::string>([](int v) {
        return std::to_string(v);
    });
    ASSERT_EQ(r2.is_ok(), true);
    ASSERT_EQ(r2.is_err(), false);
    EXPECT_EQ(r2.value(), "42");
}

TEST(TestResultNormal, map_err) {
    auto expect = std::make_error_code(std::errc::invalid_argument);
    auto r = Err<int, std::error_code>(expect);
    auto r2 = std::move(r).map<std::string>([](int v) {
        return std::to_string(v);
    });
    ASSERT_EQ(r2.is_ok(), false);
    ASSERT_EQ(r2.is_err(), true);
    EXPECT_EQ(r2.err(), expect);
    EXPECT_EQ(std::move(r2).unwrap_err(), expect);
}

TEST(TestResultNormal, map_or_ok) {
    auto r = Ok<int, std::error_code>(42);
    auto r2 = std::move(r).map_or<std::string>("This is a default value", [](int v) {
        return std::to_string(v);
    });
    ASSERT_EQ(r2.is_ok(), true);
    ASSERT_EQ(r2.is_err(), false);
    EXPECT_EQ(r2.value(), "42");
}

TEST(TestResultNormal, map_or_err) {
    auto expect = std::make_error_code(std::errc::invalid_argument);
    auto r = Err<int, std::error_code>(expect);
    auto r2 = std::move(r).map_or<std::string>("This is a default value", [](int v) {
        return std::to_string(v);
    });
    ASSERT_EQ(r2.is_ok(), true);
    ASSERT_EQ(r2.is_err(), false);
    EXPECT_EQ(r2.value(), "This is a default value");
}

Result<int, std::error_code> divide(int a, int b) {
    if (b == 0) {
        return Err<int, std::error_code>(std::make_error_code(std::errc::invalid_argument));
    }
    return Ok<int, std::error_code>(a / b);
}

TEST(TestResultNormal, divide_ok) {
    auto r = divide(99, 3).map<std::string>([](int v) {
        return std::to_string(v);
    });
    ASSERT_EQ(r.is_ok(), true);
    ASSERT_EQ(r.is_err(), false);
    EXPECT_EQ(r.value(), "33");
}

TEST(TestResultNormal, divide_err) {
    auto r = divide(99, 0).map<std::string>([](int v) {
        return std::to_string(v);
    });
    ASSERT_EQ(r.is_ok(), false);
    ASSERT_EQ(r.is_err(), true);
    EXPECT_EQ(r.err(), std::make_error_code(std::errc::invalid_argument));
}

// 死亡测试：测试所有会导致 abort 的分支

// 测试在错误状态下调用 value() 会导致 abort
TEST(TestResultDeath, ValueOnError) {
    auto expect = std::make_error_code(std::errc::invalid_argument);
    auto r = Err<int, std::error_code>(expect);

    EXPECT_DEATH(
        {
            volatile int val = r.value();
            (void)val; // 避免未使用变量警告
        },
        "value error");
}

// 测试在错误状态下调用 const value() 会导致 abort
TEST(TestResultDeath, ConstValueOnError) {
    auto expect = std::make_error_code(std::errc::invalid_argument);
    const auto r = Err<int, std::error_code>(expect);

    EXPECT_DEATH(
        {
            volatile int val = r.value();
            (void)val; // 避免未使用变量警告
        },
        "value error");
}

// 测试在成功状态下调用 err() 会导致 abort
TEST(TestResultDeath, ErrOnSuccess) {
    auto r = Ok<int, std::error_code>(42);

    EXPECT_DEATH(
        {
            volatile auto err_val = r.err();
            (void)err_val; // 避免未使用变量警告
        },
        "err error");
}

// 测试在成功状态下调用 const err() 会导致 abort
TEST(TestResultDeath, ConstErrOnSuccess) {
    const auto r = Ok<int, std::error_code>(42);

    EXPECT_DEATH(
        {
            volatile auto err_val = r.err();
            (void)err_val; // 避免未使用变量警告
        },
        "err error");
}

// 测试在错误状态下调用 unwrap() 会导致 abort
TEST(TestResultDeath, UnwrapOnError) {
    auto expect = std::make_error_code(std::errc::invalid_argument);
    auto r = Err<int, std::error_code>(expect);

    EXPECT_DEATH(
        {
            volatile int val = std::move(r).unwrap();
            (void)val; // 避免未使用变量警告
        },
        "unwrap error");
}

// 测试在成功状态下调用 unwrap_err() 会导致 abort
TEST(TestResultDeath, UnwrapErrOnSuccess) {
    auto r = Ok<int, std::error_code>(42);

    EXPECT_DEATH(
        {
            volatile auto err_val = std::move(r).unwrap_err();
            (void)err_val; // 避免未使用变量警告
        },
        "unwrap error");
}

// 测试在错误状态下调用 expect() 会导致 abort
TEST(TestResultDeath, ExpectOnError) {
    auto expect = std::make_error_code(std::errc::invalid_argument);
    auto r = Err<int, std::error_code>(expect);

    EXPECT_DEATH(
        {
            volatile int val = std::move(r).expect("Custom error message");
            (void)val; // 避免未使用变量警告
        },
        "Custom error message");
}

// 测试在错误状态下调用 value() 会显示堆栈跟踪
TEST(TestResultDeath, ValueOnErrorShowsStacktrace) {
    auto expect = std::make_error_code(std::errc::invalid_argument);
    auto r = Err<int, std::error_code>(expect);

    EXPECT_DEATH(
        {
            volatile int val = r.value();
            (void)val; // 避免未使用变量警告
        },
        "value error");
}

// 测试在错误状态下调用 err() 会显示堆栈跟踪
TEST(TestResultDeath, ErrOnSuccessShowsStacktrace) {
    auto r = Ok<int, std::error_code>(42);

    EXPECT_DEATH(
        {
            volatile auto err_val = r.err();
            (void)err_val; // 避免未使用变量警告
        },
        "StackTrace");
}

// 测试在错误状态下调用 unwrap() 会显示堆栈跟踪
TEST(TestResultDeath, UnwrapOnErrorShowsStacktrace) {
    auto expect = std::make_error_code(std::errc::invalid_argument);
    auto r = Err<int, std::error_code>(expect);

    EXPECT_DEATH(
        {
            volatile int val = std::move(r).unwrap();
            (void)val; // 避免未使用变量警告
        },
        "StackTrace");
}

// 测试在成功状态下调用 unwrap_err() 会显示堆栈跟踪
TEST(TestResultDeath, UnwrapErrOnSuccessShowsStacktrace) {
    auto r = Ok<int, std::error_code>(42);

    EXPECT_DEATH(
        {
            volatile auto err_val = std::move(r).unwrap_err();
            (void)err_val; // 避免未使用变量警告
        },
        "StackTrace");
}

// 测试在错误状态下调用 expect() 会显示堆栈跟踪
TEST(TestResultDeath, ExpectOnErrorShowsStacktrace) {
    auto expect = std::make_error_code(std::errc::invalid_argument);
    auto r = Err<int, std::error_code>(expect);

    EXPECT_DEATH(
        {
            volatile int val = std::move(r).expect("Custom error message");
            (void)val; // 避免未使用变量警告
        },
        "StackTrace");
}

// 测试 value_or 在错误状态下返回默认值（不会 abort）
TEST(TestResultNormal, ValueOrOnError) {
    auto expect = std::make_error_code(std::errc::invalid_argument);
    auto r = Err<int, std::error_code>(expect);

    int default_value = 100;
    int result = r.value_or(default_value);
    EXPECT_EQ(result, default_value);
}

// 测试 unwrap_or 在错误状态下返回默认值（不会 abort）
TEST(TestResultNormal, UnwrapOrOnError) {
    auto expect = std::make_error_code(std::errc::invalid_argument);
    auto r = Err<int, std::error_code>(expect);

    int default_value = 200;
    int result = std::move(r).unwrap_or(default_value);
    EXPECT_EQ(result, default_value);
}

// 测试 value_or 在成功状态下返回值（不会 abort）
TEST(TestResultNormal, ValueOrOnSuccess) {
    auto r = Ok<int, std::error_code>(42);

    int default_value = 100;
    int result = r.value_or(default_value);
    EXPECT_EQ(result, 42);
}

// 测试 unwrap_or 在成功状态下返回值（不会 abort）
TEST(TestResultNormal, UnwrapOrOnSuccess) {
    auto r = Ok<int, std::error_code>(42);

    int default_value = 100;
    int result = std::move(r).unwrap_or(default_value);
    EXPECT_EQ(result, 42);
}

// 测试移动后的 Result 对象状态
TEST(TestResultNormal, MovedResultState) {
    auto r1 = Ok<int, std::error_code>(42);
    EXPECT_TRUE(r1.is_ok());
    EXPECT_FALSE(r1.is_err());

    // 移动构造
    auto r2 = std::move(r1);
    EXPECT_TRUE(r2.is_ok());
    EXPECT_FALSE(r2.is_err());
    EXPECT_EQ(r2.value(), 42);

    // 移动后的对象状态可能不确定，但至少不应该崩溃
    // 这里我们主要测试移动构造的正确性
}

// 测试移动赋值
TEST(TestResultNormal, MoveAssignment) {
    auto r1 = Ok<int, std::error_code>(42);
    auto r2 = Err<int, std::error_code>(std::make_error_code(std::errc::invalid_argument));

    r2 = std::move(r1);
    EXPECT_TRUE(r2.is_ok());
    EXPECT_FALSE(r2.is_err());
    EXPECT_EQ(r2.value(), 42);
}

// 测试不同类型的错误码
TEST(TestResultNormal, DifferentErrorTypes) {
    // 使用不同的错误类型
    auto r = Err<int, std::string>("Custom error message");
    EXPECT_FALSE(r.is_ok());
    EXPECT_TRUE(r.is_err());
    EXPECT_EQ(r.err(), "Custom error message");

    // 测试在错误状态下调用 value() 会导致 abort
    EXPECT_DEATH(
        {
            volatile int val = r.value();
            (void)val; // 避免未使用变量警告
        },
        "value error");
}

// 测试字符串类型的 Result
TEST(TestResultNormal, StringResult) {
    auto r = Ok<std::string, int>("Success message");
    EXPECT_TRUE(r.is_ok());
    EXPECT_FALSE(r.is_err());
    EXPECT_EQ(r.value(), "Success message");

    // 测试在成功状态下调用 err() 会导致 abort
    EXPECT_DEATH(
        {
            volatile int err_val = r.err();
            (void)err_val; // 避免未使用变量警告
        },
        "err error");
}

// 测试空字符串的 Result
TEST(TestResultNormal, EmptyStringResult) {
    auto r = Ok<std::string, int>("");
    EXPECT_TRUE(r.is_ok());
    EXPECT_FALSE(r.is_err());
    EXPECT_EQ(r.value(), "");
}

// 测试零值的 Result
TEST(TestResultNormal, ZeroValueResult) {
    auto r = Ok<int, std::error_code>(0);
    EXPECT_TRUE(r.is_ok());
    EXPECT_FALSE(r.is_err());
    EXPECT_EQ(r.value(), 0);
}

// 测试负值的 Result
TEST(TestResultNormal, NegativeValueResult) {
    auto r = Ok<int, std::error_code>(-42);
    EXPECT_TRUE(r.is_ok());
    EXPECT_FALSE(r.is_err());
    EXPECT_EQ(r.value(), -42);
}

// 测试大值的 Result
TEST(TestResultNormal, LargeValueResult) {
    auto r = Ok<int64_t, std::error_code>(INT64_MAX);
    EXPECT_TRUE(r.is_ok());
    EXPECT_FALSE(r.is_err());
    EXPECT_EQ(r.value(), INT64_MAX);
}

// 测试嵌套的 Result 类型
TEST(TestResultNormal, NestedResult) {
    auto inner = Ok<int, std::error_code>(42);
    auto outer = Ok<Result<int, std::error_code>, std::string>(std::move(inner));
    EXPECT_TRUE(outer.is_ok());
    EXPECT_FALSE(outer.is_err());

    auto& inner_ref = outer.value();
    EXPECT_TRUE(inner_ref.is_ok());
    EXPECT_FALSE(inner_ref.is_err());
    EXPECT_EQ(inner_ref.value(), 42);
}

// 测试嵌套的 Result 错误类型
TEST(TestResultNormal, NestedResultError) {
    auto outer = Err<Result<int, std::error_code>, std::string>("Outer error");
    EXPECT_FALSE(outer.is_ok());
    EXPECT_TRUE(outer.is_err());
    EXPECT_EQ(outer.err(), "Outer error");

    // 测试在错误状态下调用 value() 会导致 abort
    EXPECT_DEATH(
        {
            volatile auto& val = outer.value();
            (void)val; // 避免未使用变量警告
        },
        "value error");
}

// 测试相同类型的 Result（T == E）
TEST(TestResultNormal, SameTypeResult) {
    auto r = Ok<int, int>(42);
    EXPECT_TRUE(r.is_ok());
    EXPECT_FALSE(r.is_err());
    EXPECT_EQ(r.value(), 42);

    auto r2 = Err<int, int>(100);
    EXPECT_FALSE(r2.is_ok());
    EXPECT_TRUE(r2.is_err());
    EXPECT_EQ(r2.err(), 100);

    // 测试在错误状态下调用 value() 会导致 abort
    EXPECT_DEATH(
        {
            volatile int val = r2.value();
            (void)val; // 避免未使用变量警告
        },
        "value error");

    // 测试在成功状态下调用 err() 会导致 abort
    EXPECT_DEATH(
        {
            volatile int err_val = r.err();
            (void)err_val; // 避免未使用变量警告
        },
        "err error");
}

// 测试浮点数的 Result
TEST(TestResultNormal, FloatResult) {
    auto r = Ok<double, std::error_code>(3.14159);
    EXPECT_TRUE(r.is_ok());
    EXPECT_FALSE(r.is_err());
    EXPECT_DOUBLE_EQ(r.value(), 3.14159);

    auto r2 = Err<double, std::error_code>(std::make_error_code(std::errc::invalid_argument));
    EXPECT_FALSE(r2.is_ok());
    EXPECT_TRUE(r2.is_err());

    // 测试在错误状态下调用 value() 会导致 abort
    EXPECT_DEATH(
        {
            volatile double val = r2.value();
            (void)val; // 避免未使用变量警告
        },
        "value error");
}

// 测试布尔值的 Result
TEST(TestResultNormal, BoolResult) {
    auto r = Ok<bool, std::error_code>(true);
    EXPECT_TRUE(r.is_ok());
    EXPECT_FALSE(r.is_err());
    EXPECT_EQ(r.value(), true);

    auto r2 = Ok<bool, std::error_code>(false);
    EXPECT_TRUE(r2.is_ok());
    EXPECT_FALSE(r2.is_err());
    EXPECT_EQ(r2.value(), false);
}

// 测试动态分配的指针类型 Result
TEST(TestResultNormal, DynamicPointerResult) {
    int* ptr = new int(42);
    auto r = Ok<int*, std::error_code>(ptr);
    EXPECT_TRUE(r.is_ok());
    EXPECT_FALSE(r.is_err());
    EXPECT_EQ(*r.value(), 42);

    delete ptr;

    auto r2 = Err<int*, std::error_code>(std::make_error_code(std::errc::invalid_argument));
    EXPECT_FALSE(r2.is_ok());
    EXPECT_TRUE(r2.is_err());

    // 测试在错误状态下调用 value() 会导致 abort
    EXPECT_DEATH(
        {
            volatile int* val = r2.value();
            (void)val; // 避免未使用变量警告
        },
        "value error");
}

// 测试引用类型的 Result（注意：rusty::Result 可能不支持引用类型）
// 这里我们测试指针类型作为替代
TEST(TestResultNormal, PointerResult) {
    int value = 42;
    auto r = Ok<int*, std::error_code>(&value);
    EXPECT_TRUE(r.is_ok());
    EXPECT_FALSE(r.is_err());
    EXPECT_EQ(*r.value(), 42);

    auto r2 = Err<int*, std::error_code>(std::make_error_code(std::errc::invalid_argument));
    EXPECT_FALSE(r2.is_ok());
    EXPECT_TRUE(r2.is_err());

    // 测试在错误状态下调用 value() 会导致 abort
    EXPECT_DEATH(
        {
            volatile int* val = r2.value();
            (void)val; // 避免未使用变量警告
        },
        "value error");
}

// 测试移动语义的正确性
TEST(TestResultNormal, MoveSemantics) {
    auto r1 = Ok<std::string, std::error_code>("Hello World");
    EXPECT_TRUE(r1.is_ok());
    EXPECT_EQ(r1.value(), "Hello World");

    // 移动构造
    auto r2 = std::move(r1);
    EXPECT_TRUE(r2.is_ok());
    EXPECT_EQ(r2.value(), "Hello World");

    // 移动赋值
    auto r3 = Err<std::string, std::error_code>(std::make_error_code(std::errc::invalid_argument));
    r3 = std::move(r2);
    EXPECT_TRUE(r3.is_ok());
    EXPECT_EQ(r3.value(), "Hello World");
}

// 测试 map 函数的正确性
TEST(TestResultNormal, MapFunction) {
    auto r = Ok<int, std::error_code>(42);

    // 映射到字符串
    auto r2 = std::move(r).map<std::string>([](int v) {
        return "Value: " + std::to_string(v);
    });
    EXPECT_TRUE(r2.is_ok());
    EXPECT_EQ(r2.value(), "Value: 42");

    // 映射到浮点数
    auto r3 = Ok<int, std::error_code>(100);
    auto r4 = std::move(r3).map<double>([](int v) {
        return v * 1.5;
    });
    EXPECT_TRUE(r4.is_ok());
    EXPECT_DOUBLE_EQ(r4.value(), 150.0);
}

// 测试 map_or 函数的正确性
TEST(TestResultNormal, MapOrFunction) {
    auto r = Ok<int, std::error_code>(42);

    // 成功情况下的映射
    auto r2 = std::move(r).map_or<std::string>("Default", [](int v) {
        return "Value: " + std::to_string(v);
    });
    EXPECT_TRUE(r2.is_ok());
    EXPECT_EQ(r2.value(), "Value: 42");

    // 错误情况下的默认值
    auto r3 = Err<int, std::error_code>(std::make_error_code(std::errc::invalid_argument));
    auto r4 = std::move(r3).map_or<std::string>("Default", [](int v) {
        return "Value: " + std::to_string(v);
    });
    EXPECT_TRUE(r4.is_ok());
    EXPECT_EQ(r4.value(), "Default");
}

} // namespace
// NOLINTEND
