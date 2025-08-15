#pragma once
#include <utility>

namespace pain {

template <typename F>
struct ScopeExit {
    F func;
    bool run;
    explicit ScopeExit(F f) noexcept : func(std::move(f)), run(true) {}
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

} // namespace pain
