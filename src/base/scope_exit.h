#pragma once
#include <utility>

namespace pain {

template <typename F>
struct scope_exit {
    F _func;
    bool _run;
    explicit scope_exit(F f) noexcept :
        _func(std::move(f)),
        _run(true) {}
    scope_exit(scope_exit &&rhs) noexcept
        :
        _func((rhs._run = false, std::move(rhs._func))),
        _run(true) {}
    ~scope_exit() {
        if (_run) {
            _func(); // RAII semantics apply, expected not to throw
        }
    }

    // "in place" construction expected, no default ctor provided either
    // also unclear what should be done with the old functor, should it
    // be called since it is no longer needed, or not since *this is not
    // going out of scope just yet...
    scope_exit &operator=(scope_exit &&rhs) = delete;
    // to be explicit...
    scope_exit(scope_exit const &) = delete;
    scope_exit &operator=(scope_exit const &) = delete;
};

template <typename F>
scope_exit<F> make_scope_exit(F &&f) noexcept {
    return scope_exit<F>{std::forward<F>(f)};
}

} // namespace pain
