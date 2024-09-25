#pragma once

#include <functional>
#include <iostream>
#include <variant>

#include <boost/stacktrace.hpp>

namespace pain {
template <typename T, typename E>
    requires(!std::is_same_v<T, E>)
class [[nodiscard]] Result {
public:
    constexpr Result(T v) : _value(std::move(v)) {}
    constexpr Result(E e) : _value(std::move(e)) {}

    constexpr Result(Result&& other) noexcept = default;
    constexpr Result& operator=(Result&& other) noexcept = default;

    constexpr T& value() & noexcept {
        if (is_ok()) {
            return std::get<T>(_value);
        } else {
            std::cerr << "value error" << std::endl;
            std::cerr << "Backtrace: " << std::endl;
            std::cerr << boost::stacktrace::stacktrace() << std::endl;
            abort();
        }
    }

    constexpr const T& value() const& noexcept {
        if (is_ok()) {
            return std::get<T>(_value);
        } else {
            std::cerr << "value error" << std::endl;
            std::cerr << "Backtrace: " << std::endl;
            std::cerr << boost::stacktrace::stacktrace() << std::endl;
            abort();
        }
    }

    template <typename U>
    constexpr T value_or(U&& v) & noexcept {
        if (is_ok()) {
            return std::get<T>(_value);
        } else {
            return std::forward<U>(v);
        }
    }

    constexpr E& err() & noexcept {
        if (!is_ok()) {
            return std::get<E>(_value);
        } else {
            std::cerr << "err error" << std::endl;
            std::cerr << "Backtrace: " << std::endl;
            std::cerr << boost::stacktrace::stacktrace() << std::endl;
            abort();
        }
    }

    constexpr const E& err() const& noexcept {
        if (!is_ok()) {
            return std::get<E>(_value);
        } else {
            std::cerr << "err error" << std::endl;
            std::cerr << "Backtrace: " << std::endl;
            std::cerr << boost::stacktrace::stacktrace() << std::endl;
            abort();
        }
    }

    constexpr T unwrap() && noexcept {
        if (is_ok()) {
            return std::get<T>(std::move(_value));
        } else {
            std::cerr << "unwrap error" << std::endl;
            std::cerr << "Backtrace: " << std::endl;
            std::cerr << boost::stacktrace::stacktrace() << std::endl;
            abort();
        }
    }

    constexpr T unwrap_or(T v) && noexcept {
        if (is_ok()) {
            return std::get<T>(std::move(_value));
        } else {
            return v;
        }
    }

    constexpr T expect(const char* msg) && noexcept {
        if (is_ok()) {
            return std::get<T>(std::move(_value));
        } else {
            std::cerr << msg << std::endl;
            std::cerr << "Backtrace: " << std::endl;
            std::cerr << boost::stacktrace::stacktrace() << std::endl;
            abort();
        }
    }

    constexpr E unwrap_err() && noexcept {
        if (!is_ok()) {
            return std::get<E>(std::move(_value));
        } else {
            std::cerr << "err error" << std::endl;
            std::cerr << "Backtrace: " << std::endl;
            std::cerr << boost::stacktrace::stacktrace() << std::endl;
            abort();
        }
    }

    template <typename U, typename F>
    constexpr Result<U, E> map(F&& f) && {
        if (is_ok()) {
            return Result<U, E>(std::invoke(std::forward<F>(f), std::get<T>(std::move(_value))));
        } else {
            return Result<U, E>(std::get<E>(std::move(_value)));
        }
    }

    template <typename U, typename F>
    constexpr Result<U, E> map_or(U&& u, F&& f) && {
        if (is_ok()) {
            return Result<U, E>(std::invoke(std::forward<F>(f), std::get<T>(std::move(_value))));
        } else {
            return Result<U, E>(std::forward<U>(u));
        }
    }

    constexpr bool is_ok() const {
        return std::holds_alternative<T>(_value);
    }
    constexpr bool is_err() const {
        return std::holds_alternative<E>(_value);
    }

private:
    std::variant<T, E> _value;
};

} // namespace pain
