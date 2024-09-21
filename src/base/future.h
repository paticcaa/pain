#pragma once

#include <bthread/bthread.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <stdlib.h>

#include <exception>
#include <memory>
#include <mutex>

#include <boost/assert.hpp>

namespace pain {

template <typename T>
class Promise;

template <>
class Promise<void>;

template <typename T>
class Future;

template <>
class Future<void>;

template <typename T>
struct FutureState {
    enum class State {
        Future,
        Result,
        Exception,
    };

    union any {
        any() {}
        ~any() {}
        T value;
        std::exception_ptr ex;
    } u;

    ~FutureState() {
        std::lock_guard guard(_mutex);
        switch (state) {
        case State::Future: break;
        case State::Result: u.value.~T(); break;
        case State::Exception: u.ex.~exception_ptr(); break;
        default: abort();
        }
    }

    bool failed() const {
        std::lock_guard guard(_mutex);
        return state == State::Exception;
    }

    template <typename A>
    void set_value(A&& value) {
        std::lock_guard guard(_mutex);
        BOOST_ASSERT(state == State::Future);
        state = State::Result;
        new (&u.value) T(std::forward<A>(value));
        _cond.notify_one();
    }

    void set_exception(std::exception_ptr ex) {
        std::lock_guard guard(_mutex);
        state = State::Exception;
        u.ex = std::move(ex);
        _cond.notify_one();
    }

    T get() {
        std::unique_lock guard(_mutex);
        while (state == State::Future) {
            _cond.wait(guard);
        }
        if (state == State::Exception) {
            std::rethrow_exception(u.ex);
        }
        return std::move(u.value);
    }

    bool available() const {
        std::lock_guard guard(_mutex);
        return state == State::Result || state == State::Exception;
    }

    State state = State::Future;
    Promise<T>* promise = nullptr;

    mutable bthread::Mutex _mutex;
    mutable bthread::ConditionVariable _cond;
};

template <>
struct FutureState<void> {
    enum class State {
        Future,
        Result,
        Exception,
    };

    union any {
        any() {}
        ~any() {}
        std::exception_ptr ex;
    } u;

    ~FutureState() {
        std::lock_guard guard(_mutex);
        switch (state) {
        case State::Future: break;
        case State::Result: break;
        case State::Exception: u.ex.~exception_ptr(); break;
        default: abort();
        }
    }

    bool failed() const {
        std::lock_guard guard(_mutex);
        return state == State::Exception;
    }

    void set_value() {
        std::lock_guard guard(_mutex);
        BOOST_ASSERT(state == State::Future);
        state = State::Result;
        _cond.notify_one();
    }

    void set_exception(std::exception_ptr ex) {
        std::lock_guard guard(_mutex);
        state = State::Exception;
        u.ex = std::move(ex);
        _cond.notify_one();
    }

    void get() {
        std::unique_lock guard(_mutex);
        while (state == State::Future) {
            _cond.wait(guard);
        }
        if (state == State::Exception) {
            std::rethrow_exception(u.ex);
        }
        return;
    }

    bool available() const {
        std::lock_guard guard(_mutex);
        return state == State::Result || state == State::Exception;
    }

    State state = State::Future;
    Promise<void>* promise = nullptr;

    mutable bthread::Mutex _mutex;
    mutable bthread::ConditionVariable _cond;
};

template <typename T>
class Future {
public:
    Future() = default;
    Future(Future&& rhs) {
        _state = std::move(rhs._state);
    }

    Future(std::shared_ptr<FutureState<T>> state) : _state(state) {}

    ~Future() = default;

    Future& operator=(Future&& rhs) {
        if (this == &rhs) {
            return *this;
        }
        _state = std::move(rhs._state);
        return *this;
    }

    T get() {
        return _state->get();
    }

private:
    std::shared_ptr<FutureState<T>> _state;
};

template <>
class Future<void> {
public:
    Future() = default;
    Future(Future&& rhs) {
        _state = std::move(rhs._state);
    }

    Future(std::shared_ptr<FutureState<void>> state) : _state(state) {}

    ~Future() = default;

    Future& operator=(Future&& rhs) {
        if (this == &rhs) {
            return *this;
        }
        _state = std::move(rhs._state);
        return *this;
    }

    void get() {
        return _state->get();
    }

private:
    std::shared_ptr<FutureState<void>> _state;
};

template <typename T>
class Promise {
public:
    Promise(const Promise&) = delete;
    Promise() : _state(std::make_shared<FutureState<T>>()) {
        _state->promise = this;
    }

    ~Promise() {
        if (_state && !_state->available()) {
            _state->set_exception(std::make_exception_ptr(std::runtime_error("Broken Promise")));
        }
        if (_state) {
            _state->promise = nullptr;
        }
    }

    Promise(Promise&& other) noexcept {
        if (_state && !_state->available()) {
            _state->set_exception(std::make_exception_ptr(std::runtime_error("Broken Promise")));
        }
        _state = std::move(other._state);
        _state->promise = this;
    }

    Promise& operator=(Promise&& other) noexcept {
        if (this == &other) {
            return *this;
        }
        if (_state && !_state->available()) {
            _state->set_exception(std::make_exception_ptr(std::runtime_error("Broken Promise")));
        }
        _state = std::move(other._state);
        _state->promise = this;
        return *this;
    }

    Future<T> get_future() {
        BOOST_ASSERT(_state);
        return Future<T>(_state);
    }

    template <typename U>
    void set_value(U&& value) {
        _state->set_value(std::forward<U>(value));
    }

    void set_exception(std::exception_ptr ex) {
        _state->set_exception(std::move(ex));
    }

    bool is_ready() const {
        return _state->available();
    }

private:
    std::shared_ptr<FutureState<T>> _state;
};

template <>
class Promise<void> {
public:
    Promise(const Promise&) = delete;
    Promise() : _state(std::make_shared<FutureState<void>>()) {
        _state->promise = this;
    }

    ~Promise() {
        if (_state && !_state->available()) {
            _state->set_exception(std::make_exception_ptr(std::runtime_error("Broken Promise")));
        }
        if (_state) {
            _state->promise = nullptr;
        }
    }

    Promise(Promise&& other) noexcept {
        if (_state && !_state->available()) {
            _state->set_exception(std::make_exception_ptr(std::runtime_error("Broken Promise")));
        }
        _state = std::move(other._state);
        _state->promise = this;
    }

    Promise& operator=(Promise&& other) noexcept {
        if (this == &other) {
            return *this;
        }
        if (_state && !_state->available()) {
            _state->set_exception(std::make_exception_ptr(std::runtime_error("Broken Promise")));
        }
        _state = std::move(other._state);
        _state->promise = this;
        return *this;
    }

    Future<void> get_future() {
        BOOST_ASSERT(_state);
        return Future<void>(_state);
    }

    void set_value() {
        _state->set_value();
    }

    void set_exception(std::exception_ptr ex) {
        _state->set_exception(std::move(ex));
    }

private:
    std::shared_ptr<FutureState<void>> _state;
};

template <typename T>
Future<T> make_ready_future(T&& value) {
    Promise<T> p;
    p.set_value(std::forward<T>(value));
    return p.get_future();
}

inline Future<void> make_ready_future() {
    Promise<void> p;
    p.set_value();
    return p.get_future();
}

} // namespace pain
