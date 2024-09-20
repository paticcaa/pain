
#pragma once

#include <bthread/bthread.h>

namespace pain::base {
template <typename T>
class BthreadLocal {
public:
    BthreadLocal() {
        bthread_key_create(&_tls_key, [](void *p) { delete (T *)p; });
    }

    T *get() const {
        return (T *)bthread_getspecific(_tls_key);
    }

    T *get_or_create() {
        T *p = get();
        if (p == nullptr) {
            p = new T();
            set(p);
        }
        return p;
    }

    T *operator->() {
        return get_or_create();
    }

    T &operator*() {
        return *get_or_create();
    }

private:
    // User can't call set(), that may cause memory leak
    int set(T *p) {
        return bthread_setspecific(_tls_key, p);
    }

    bthread_key_t _tls_key;
};

} // namespace pain::base
