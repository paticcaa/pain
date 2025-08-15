#include <gtest/gtest.h>
#include <system_error>
#include "base/result.h"

// NOLINTBEGIN
using namespace pain::rusty;

TEST(Result, ok) {
    auto r = Ok<int, std::error_code>(42);
    ASSERT_EQ(r.is_ok(), true);
    ASSERT_EQ(r.is_err(), false);
    EXPECT_EQ(r.value(), 42);
    EXPECT_EQ(std::move(r).unwrap(), 42);
}

TEST(Result, err) {
    auto expect = std::make_error_code(std::errc::invalid_argument);
    auto r = Err<int, std::error_code>(expect);
    ASSERT_EQ(r.is_ok(), false);
    ASSERT_EQ(r.is_err(), true);
    EXPECT_EQ(r.err(), expect);
    EXPECT_EQ(std::move(r).unwrap_err(), expect);
}

TEST(Result, map_ok) {
    auto r = Ok<int, std::error_code>(42);
    auto r2 = std::move(r).map<std::string>([](int v) {
        return std::to_string(v);
    });
    ASSERT_EQ(r2.is_ok(), true);
    ASSERT_EQ(r2.is_err(), false);
    EXPECT_EQ(r2.value(), "42");
}

TEST(Result, map_err) {
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

TEST(Result, map_or_ok) {
    auto r = Ok<int, std::error_code>(42);
    auto r2 = std::move(r).map_or<std::string>("This is a default value", [](int v) {
        return std::to_string(v);
    });
    ASSERT_EQ(r2.is_ok(), true);
    ASSERT_EQ(r2.is_err(), false);
    EXPECT_EQ(r2.value(), "42");
}

TEST(Result, map_or_err) {
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

TEST(Result, divide_ok) {
    auto r = divide(99, 3).map<std::string>([](int v) {
        return std::to_string(v);
    });
    ASSERT_EQ(r.is_ok(), true);
    ASSERT_EQ(r.is_err(), false);
    EXPECT_EQ(r.value(), "33");
}

TEST(Result, divide_err) {
    auto r = divide(99, 0).map<std::string>([](int v) {
        return std::to_string(v);
    });
    ASSERT_EQ(r.is_ok(), false);
    ASSERT_EQ(r.is_err(), true);
    EXPECT_EQ(r.err(), std::make_error_code(std::errc::invalid_argument));
}
// NOLINTEND
