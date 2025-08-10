#include <iostream>

#include <butil/debug/debugger.h>
#include <butil/debug/stack_trace.h>
#include <boost/stacktrace.hpp>
#include <spdlog/spdlog.h>

namespace boost {
__attribute__((weak)) void assertion_failed(char const* expr, char const* function, char const* file, long line) {
    spdlog::apply_all([](auto logger) {
        logger->flush();
    });
    std::cerr << "Assertion failed: " << expr << " in function " << function << " at " << file << ":" << line
              << std::endl;
    butil::debug::StackTrace trace;
    trace.Print();
    butil::debug::BreakDebugger();
}

__attribute__((weak)) void
assertion_failed_msg(char const* expr, char const* msg, char const* function, char const* file, long line) {
    spdlog::apply_all([](auto logger) {
        logger->flush();
    });
    std::cerr << "Assertion failed: " << expr << " (" << msg << ") in function " << function << " at " << file << ":"
              << line << std::endl;
    butil::debug::StackTrace trace;
    trace.Print();
    butil::debug::BreakDebugger();
}
} // namespace boost
