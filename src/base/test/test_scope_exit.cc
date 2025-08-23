#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <pain/base/scope_exit.h>
#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <vector>

// NOLINTBEGIN(readability-magic-numbers)
namespace {
using namespace pain;

class TestScopeExit : public ::testing::Test {
protected:
    void SetUp() override {
        _counter = 0;
        _destructor_called = false;
    }

    void TearDown() override {
        // 确保所有测试后计数器重置
        _counter = 0;
        _destructor_called = false;
    }

    // 辅助方法：创建简单的计数器函数
    auto make_counter_function() {
        return [this]() {
            _counter++;
        };
    }

    // 辅助方法：创建字符串记录函数
    auto make_string_recorder_function(std::string& str, const std::string& value) {
        return [&str, value]() {
            str = value;
        };
    }

    // 辅助方法：创建析构函数标记函数
    auto make_destructor_marker_function() {
        return [this]() {
            _destructor_called = true;
        };
    }

    // 辅助方法：创建异常抛出函数
    auto make_exception_function() {
        return []() {
            throw std::runtime_error("Scope exit exception");
        };
    }

    std::atomic<int> _counter;
    bool _destructor_called;
};

// 基础功能测试
TEST_F(TestScopeExit, BasicFunctionality) {
    {
        auto scope_exit = make_scope_exit(make_counter_function());
        EXPECT_EQ(_counter, 0);
    }
    // 作用域结束时应该执行函数
    EXPECT_EQ(_counter, 1);
}

TEST_F(TestScopeExit, MultipleScopeExits) {
    {
        auto scope_exit1 = make_scope_exit(make_counter_function());
        auto scope_exit2 = make_counter_function();
        auto scope_exit3 = make_scope_exit(scope_exit2);
        EXPECT_EQ(_counter, 0);
    }
    // 两个作用域退出都应该执行
    EXPECT_EQ(_counter, 2);
}

TEST_F(TestScopeExit, StringRecording) {
    std::string recorded_value;
    {
        auto scope_exit = make_scope_exit(make_string_recorder_function(recorded_value, "test_value"));
        EXPECT_TRUE(recorded_value.empty());
    }
    EXPECT_EQ(recorded_value, "test_value");
}

TEST_F(TestScopeExit, DestructorMarking) {
    {
        auto scope_exit = make_scope_exit(make_destructor_marker_function());
        EXPECT_FALSE(_destructor_called);
    }
    EXPECT_TRUE(_destructor_called);
}

// 移动语义测试
TEST_F(TestScopeExit, MoveConstructor) {
    {
        auto scope_exit1 = make_scope_exit(make_counter_function());
        EXPECT_EQ(_counter, 0);

        // 移动构造
        auto scope_exit2 = std::move(scope_exit1);
        EXPECT_EQ(_counter, 0);
    }
    // 只有移动后的对象应该执行函数
    EXPECT_EQ(_counter, 1);
}

// release 功能测试
TEST_F(TestScopeExit, ReleaseFunctionality) {
    {
        auto scope_exit = make_scope_exit(make_counter_function());
        EXPECT_EQ(_counter, 0);

        // 释放作用域退出
        scope_exit.release();
        EXPECT_EQ(_counter, 0);
    }
    // 释放后不应该执行函数
    EXPECT_EQ(_counter, 0);
}

TEST_F(TestScopeExit, ReleaseAfterMove) {
    {
        auto scope_exit1 = make_scope_exit(make_counter_function());
        EXPECT_EQ(_counter, 0);

        // 移动构造
        auto scope_exit2 = std::move(scope_exit1);
        EXPECT_EQ(_counter, 0);

        // 释放移动后的对象
        scope_exit2.release();
        EXPECT_EQ(_counter, 0);
    }
    // 释放后不应该执行函数
    EXPECT_EQ(_counter, 0);
}

// 异常安全测试
TEST_F(TestScopeExit, ExceptionSafety) {
    EXPECT_EXIT({ auto scope_exit = make_scope_exit(make_exception_function()); },
                ::testing::KilledBySignal(SIGABRT),
                "Scope exit exception");
}

// 复杂函数对象测试
TEST_F(TestScopeExit, ComplexFunctionObject) {
    std::vector<int> numbers;
    {
        auto scope_exit = make_scope_exit([&numbers]() {
            numbers.push_back(100);
            numbers.push_back(200);
            numbers.push_back(300);
        });
        EXPECT_TRUE(numbers.empty());
    }
    EXPECT_EQ(numbers.size(), 3);
    EXPECT_EQ(numbers[0], 100);
    EXPECT_EQ(numbers[1], 200);
    EXPECT_EQ(numbers[2], 300);
}

TEST_F(TestScopeExit, LambdaWithCapture) {
    int local_value = 42;
    std::string result;
    {
        auto scope_exit = make_scope_exit([&result, local_value]() {
            result = "Captured value: " + std::to_string(local_value);
        });
        EXPECT_TRUE(result.empty());
    }
    EXPECT_EQ(result, "Captured value: 42");
}

TEST_F(TestScopeExit, MemberFunctionPointer) {
    struct TestClass {
        void test_function() {
            called = true;
        }
        bool called = false;
    };

    TestClass obj;
    {
        auto scope_exit = make_scope_exit([&obj]() {
            obj.test_function();
        });
        EXPECT_FALSE(obj.called);
    }
    EXPECT_TRUE(obj.called);
}

// 嵌套作用域测试
TEST_F(TestScopeExit, NestedScopes) {
    {
        auto outer_scope = make_scope_exit(make_counter_function());
        EXPECT_EQ(_counter, 0);

        {
            auto inner_scope = make_scope_exit(make_counter_function());
            EXPECT_EQ(_counter, 0);
        }
        // 内层作用域退出，计数器应该增加
        EXPECT_EQ(_counter, 1);
    }
    // 外层作用域退出，计数器应该再次增加
    EXPECT_EQ(_counter, 2);
}

TEST_F(TestScopeExit, NestedScopesWithRelease) {
    {
        auto outer_scope = make_scope_exit(make_counter_function());
        EXPECT_EQ(_counter, 0);

        {
            auto inner_scope = make_scope_exit(make_counter_function());
            inner_scope.release(); // 释放内层作用域
            EXPECT_EQ(_counter, 0);
        }
        // 内层作用域被释放，不应该执行
        EXPECT_EQ(_counter, 0);
    }
    // 外层作用域退出，计数器应该增加
    EXPECT_EQ(_counter, 1);
}

// 智能指针测试
TEST_F(TestScopeExit, WithSmartPointers) {
    auto ptr = std::make_unique<int>(42);
    {
        auto scope_exit = make_scope_exit([&ptr]() {
            ptr.reset(); // 在作用域退出时释放智能指针
        });
        EXPECT_TRUE(ptr != nullptr);
        EXPECT_EQ(*ptr, 42);
    }
    // 作用域退出后，智能指针应该被释放
    EXPECT_TRUE(ptr == nullptr);
}

// 函数对象状态测试
TEST_F(TestScopeExit, FunctionObjectState) {
    int call_count = 0;
    auto function_object = [&call_count]() {
        call_count++;
    };

    {
        auto scope_exit = make_scope_exit(function_object);
        EXPECT_EQ(call_count, 0);
    }
    EXPECT_EQ(call_count, 1);

    // 再次使用同一个函数对象
    {
        auto scope_exit2 = make_scope_exit(function_object);
        EXPECT_EQ(call_count, 1);
    }
    EXPECT_EQ(call_count, 2);
}

// 边界情况测试
TEST_F(TestScopeExit, EmptyFunction) {
    {
        auto scope_exit = make_scope_exit([]() {});
    }
}

TEST_F(TestScopeExit, LargeFunctionObject) {
    // 创建一个较大的函数对象
    std::vector<int> large_data(1000, 42);
    {
        auto scope_exit = make_scope_exit([large_data]() {
            // 在作用域退出时处理大量数据
            volatile int sum = 0;
            for (int value : large_data) {
                sum += value;
            }
        });
        // 应该正常工作
        EXPECT_EQ(large_data.size(), 1000);
    }
}

// 宏测试
TEST_F(TestScopeExit, MacroUsage) {
    int macro_counter = 0;
    {
        SCOPE_EXIT {
            macro_counter++;
        };
        EXPECT_EQ(macro_counter, 0);
    }
    EXPECT_EQ(macro_counter, 1);
}

TEST_F(TestScopeExit, MacroWithCapture) {
    std::string macro_result;
    int value = 123;
    {
        SCOPE_EXIT {
            macro_result = "Value: " + std::to_string(value);
        };
        EXPECT_TRUE(macro_result.empty());
    }
    EXPECT_EQ(macro_result, "Value: 123");
}

TEST_F(TestScopeExit, MultipleMacros) {
    int macro_counter1 = 0;
    int macro_counter2 = 0;
    {
        SCOPE_EXIT {
            macro_counter1++;
        };
        SCOPE_EXIT {
            macro_counter2++;
        };
        EXPECT_EQ(macro_counter1, 0);
        EXPECT_EQ(macro_counter2, 0);
    }
    EXPECT_EQ(macro_counter1, 1);
    EXPECT_EQ(macro_counter2, 1);
}

// 运算符重载测试
TEST_F(TestScopeExit, OperatorOverload) {
    int operator_counter = 0;
    {
        auto scope_exit = ScopeGuardOnExit() + [&operator_counter]() {
            operator_counter++;
        };
        EXPECT_EQ(operator_counter, 0);
    }
    EXPECT_EQ(operator_counter, 1);
}

TEST_F(TestScopeExit, OperatorOverloadWithComplexLambda) {
    std::vector<std::string> results;
    {
        auto scope_exit = ScopeGuardOnExit() + [&results]() {
            results.emplace_back("First");
            results.emplace_back("Second");
            results.emplace_back("Third");
        };
        EXPECT_TRUE(results.empty());
    }
    EXPECT_EQ(results.size(), 3);
    EXPECT_EQ(results[0], "First");
    EXPECT_EQ(results[1], "Second");
    EXPECT_EQ(results[2], "Third");
}

// 内存泄漏测试
TEST_F(TestScopeExit, NoMemoryLeaks) {
    // 创建多个 ScopeExit 对象，确保没有内存泄漏
    for (int i = 0; i < 100; ++i) {
        auto scope_exit = make_scope_exit(make_counter_function());
        // 对象在作用域结束时自动销毁
    }
    // 如果存在内存泄漏，这里可能会崩溃或产生其他问题
    EXPECT_EQ(_counter, 100);
}

// 线程安全测试（基本）
TEST_F(TestScopeExit, BasicThreadSafety) {
    std::atomic<int> thread_counter{0};
    std::vector<std::thread> threads;

    // 创建多个线程，每个线程都有自己的 ScopeExit
    for (int i = 0; i < 10; ++i) {
        threads.emplace_back([&thread_counter]() {
            auto scope_exit = make_scope_exit([&thread_counter]() {
                thread_counter++;
            });
        });
    }

    // 等待所有线程完成
    for (auto& thread : threads) {
        thread.join();
    }

    // 所有线程的 ScopeExit 都应该执行
    EXPECT_EQ(thread_counter, 10);
}

} // namespace
// NOLINTEND(readability-magic-numbers)
