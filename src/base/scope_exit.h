#pragma once
#include <utility>
#include <boost/preprocessor/cat.hpp>

namespace pain {

template <typename F>
struct ScopeExit {
    F func;
    bool run;
    explicit ScopeExit(F&& f) noexcept : func(std::forward<F>(f)), run(true) {}
    ScopeExit(ScopeExit&& rhs) noexcept : func((rhs.run = false, std::move(rhs.func))), run(true) {}
    ~ScopeExit() noexcept {
        if (run) {
            func();
        }
    }

    ScopeExit& operator=(ScopeExit&& rhs) = delete;
    ScopeExit(ScopeExit const&) = delete;
    ScopeExit& operator=(ScopeExit const&) = delete;
    void release() noexcept {
        run = false;
    }
};

template <typename F>
ScopeExit<F> make_scope_exit(F&& f) noexcept {
    return ScopeExit<F>{std::forward<F>(f)};
}

enum class ScopeGuardOnExit {
};
template <typename FunctionType>
ScopeExit<std::decay_t<FunctionType>> operator+(ScopeGuardOnExit, FunctionType&& fn) {
    return ScopeExit<std::decay_t<FunctionType>>(std::forward<FunctionType>(fn));
}

} // namespace pain

#define SCOPE_EXIT auto BOOST_PP_CAT(__scope_exit_, __LINE__) = pain::ScopeGuardOnExit() + [&]() noexcept
