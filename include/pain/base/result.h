#pragma once

#include <functional>
#include <iostream>
#include <variant>

#include <boost/assert.hpp>

namespace pain {

template <typename T, typename E>
class [[nodiscard]] Result {
public:
    struct Ok {
        T value;
    };
    struct Err {
        E value;
    };

    constexpr Result(Ok v) : _value(std::move(v)) {}
    constexpr Result(Err e) : _value(std::move(e)) {}

    constexpr Result(Result&& other) noexcept = default;
    constexpr Result& operator=(Result&& other) noexcept = default;

    constexpr T& value() & noexcept {
        BOOST_ASSERT_MSG(is_ok(), "value error");
        return std::get<Ok>(_value).value;
    }

    constexpr const T& value() const& noexcept {
        BOOST_ASSERT_MSG(is_ok(), "value error");
        return std::get<Ok>(_value).value;
    }

    template <typename U>
    constexpr T value_or(U&& v) & noexcept {
        if (is_ok()) {
            return std::get<Ok>(_value).value;
        }
        return std::forward<U>(v);
    }

    constexpr E& err() & noexcept {
        BOOST_ASSERT_MSG(!is_ok(), "err error");
        return std::get<Err>(_value).value;
    }

    constexpr const E& err() const& noexcept {
        BOOST_ASSERT_MSG(!is_ok(), "err error");
        return std::get<Err>(_value).value;
    }

    constexpr T unwrap() && noexcept {
        BOOST_ASSERT_MSG(is_ok(), "unwrap error");
        return std::get<Ok>(std::move(_value)).value;
    }

    constexpr T unwrap_or(T v) && noexcept {
        if (is_ok()) {
            return std::get<Ok>(std::move(_value)).value;
        }
        return v;
    }

    constexpr T expect(const char* msg) && noexcept {
        BOOST_ASSERT_MSG(is_ok(), msg);
        return std::get<Ok>(std::move(_value)).value;
    }

    constexpr E unwrap_err() && noexcept {
        BOOST_ASSERT_MSG(!is_ok(), "unwrap error");
        return std::get<Err>(std::move(_value)).value;
    }

    template <typename U, typename F>
    constexpr Result<U, E> map(F&& f) && {
        if (is_ok()) {
            return Result<U, E>(
                typename Result<U, E>::Ok(std::invoke(std::forward<F>(f), std::get<Ok>(std::move(_value)).value)));
        }
        return Result<U, E>(typename Result<U, E>::Err(std::get<Err>(std::move(_value)).value));
    }

    template <typename U, typename F>
    constexpr Result<U, E> map_or(U&& u, F&& f) && {
        if (is_ok()) {
            return Result<U, E>(
                typename Result<U, E>::Ok(std::invoke(std::forward<F>(f), std::get<Ok>(std::move(_value)).value)));
        }
        return Result<U, E>(typename Result<U, E>::Ok(std::forward<U>(u)));
    }

    constexpr bool is_ok() const {
        return std::holds_alternative<Ok>(_value);
    }
    constexpr bool is_err() const {
        return std::holds_alternative<Err>(_value);
    }

private:
    std::variant<Ok, Err> _value;
};

template <typename T, typename E>
constexpr Result<T, E> Ok(T v) { // NOLINT(readability-identifier-naming)
    return Result<T, E>(typename Result<T, E>::Ok{std::move(v)});
}

template <typename T, typename E>
constexpr Result<T, E> Err(E e) { // NOLINT(readability-identifier-naming)
    return Result<T, E>(typename Result<T, E>::Err{std::move(e)});
}
} // namespace pain
