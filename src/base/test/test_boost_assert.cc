#include <gtest/gtest.h>
#include <boost/assert.hpp>

// NOLINTBEGIN
namespace {

// 测试 BOOST_ASSERT 的基本功能
TEST(TestBoostAssert, BasicAssertion) {
    // 这个断言应该通过
    BOOST_ASSERT(true);
    BOOST_ASSERT(1 == 1);
    BOOST_ASSERT(42 > 0);

    // 这些断言应该失败并导致程序崩溃
    EXPECT_DEATH({ BOOST_ASSERT(false); }, "Assertion failed: false");

    EXPECT_DEATH({ BOOST_ASSERT(1 == 0); }, "Assertion failed: 1 == 0");

    EXPECT_DEATH({ BOOST_ASSERT(42 < 0); }, "Assertion failed: 42 < 0");
}

// 测试 BOOST_ASSERT_MSG 的基本功能
TEST(TestBoostAssert, MessageAssertion) {
    // 这个断言应该通过
    BOOST_ASSERT_MSG(true, "This should not fail");
    BOOST_ASSERT_MSG(1 == 1, "One equals one");
    BOOST_ASSERT_MSG(42 > 0, "Positive number");

    // 这些断言应该失败并导致程序崩溃，同时显示自定义消息
    EXPECT_DEATH({ BOOST_ASSERT_MSG(false, "This is a custom error message"); },
                 "Assertion failed: false \\(This is a custom error message\\)");

    EXPECT_DEATH({ BOOST_ASSERT_MSG(1 == 0, "One does not equal zero"); },
                 "Assertion failed: 1 == 0 \\(One does not equal zero\\)");

    EXPECT_DEATH({ BOOST_ASSERT_MSG(42 < 0, "Forty-two is not negative"); },
                 "Assertion failed: 42 < 0 \\(Forty-two is not negative\\)");
}

} // namespace
// NOLINTEND
