#pragma once
#include <utility>

namespace pain {

template <typename F>
struct ScopeExit {
    F _func;
    bool _run;
    explicit ScopeExit(F f) noexcept : _func(std::move(f)), _run(true) {}
    ScopeExit(ScopeExit&& rhs) noexcept : _func((rhs._run = false, std::move(rhs._func))), _run(true) {}
    ~ScopeExit() noexcept {
        if (_run) {
            _func();
        }
    }

    ScopeExit& operator=(ScopeExit&& rhs) = delete;
    ScopeExit(ScopeExit const&) = delete;
    ScopeExit& operator=(ScopeExit const&) = delete;
    void release() noexcept {
        _run = false;
    }
};

template <typename F>
ScopeExit<F> make_scope_exit(F&& f) noexcept {
    return ScopeExit<F>{std::forward<F>(f)};
}

} // namespace pain
