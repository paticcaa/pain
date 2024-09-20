#include "base/bthread_local_context_storage.h"

#include <spdlog/spdlog.h>

namespace pain {

BthreadLocalContextStorage::BthreadLocalContextStorage() noexcept {
    SPDLOG_INFO("BthreadLocalContextStorage init");
}

BthreadLocalContextStorage::~BthreadLocalContextStorage() noexcept {
    SPDLOG_INFO("BthreadLocalContextStorage exit");
}

// Return the current context.
opentelemetry::context::Context
BthreadLocalContextStorage::GetCurrent() noexcept {
    return GetStack().Top();
}

// Resets the context to the value previous to the passed in token. This will
// also detach all child contexts of the passed in token.
// Returns true if successful, false otherwise.
bool BthreadLocalContextStorage::Detach(
    opentelemetry::context::Token& token) noexcept {
    // In most cases, the context to be detached is on the top of the stack.
    if (token == GetStack().Top()) {
        GetStack().Pop();
        return true;
    }

    if (!GetStack().Contains(token)) {
        return false;
    }

    while (!(token == GetStack().Top())) {
        GetStack().Pop();
    }

    GetStack().Pop();

    return true;
}

// Sets the current 'Context' object. Returns a token
// that can be used to reset to the previous Context.
std::unique_ptr<opentelemetry::context::Token>
BthreadLocalContextStorage::Attach(
    const opentelemetry::context::Context& context) noexcept {
    GetStack().Push(context);
    return CreateToken(context);
}

BthreadLocalContextStorage::Stack::Stack() noexcept
    :
    size_(0),
    capacity_(0),
    base_(nullptr) {}

// Pops the top Context off the stack.
void BthreadLocalContextStorage::Stack::Pop() noexcept {
    if (size_ == 0) {
        return;
    }
    // Store empty Context before decrementing `size`, to ensure
    // the shared_ptr object (if stored in prev context object ) are released.
    // The stack is not resized, and the unused memory would be reutilised
    // for subsequent context storage.
    base_[size_ - 1] = opentelemetry::context::Context();
    size_ -= 1;
}

bool BthreadLocalContextStorage::Stack::Contains(
    const opentelemetry::context::Token& token) const noexcept {
    for (size_t pos = size_; pos > 0; --pos) {
        if (token == base_[pos - 1]) {
            return true;
        }
    }

    return false;
}

// Returns the Context at the top of the stack.
opentelemetry::context::Context
BthreadLocalContextStorage::Stack::Top() const noexcept {
    if (size_ == 0) {
        return opentelemetry::context::Context();
    }
    return base_[size_ - 1];
}

// Pushes the passed in context pointer to the top of the stack
// and resizes if necessary.
void BthreadLocalContextStorage::Stack::Push(
    const opentelemetry::context::Context& context) noexcept {
    size_++;
    if (size_ > capacity_) {
        Resize(size_ * 2);
    }
    base_[size_ - 1] = context;
}

// Reallocates the storage array to the pass in new capacity size.
void BthreadLocalContextStorage::Stack::Resize(size_t new_capacity) noexcept {
    size_t old_size = size_ - 1;
    if (new_capacity == 0) {
        new_capacity = 2;
    }
    opentelemetry::context::Context* temp =
        new opentelemetry::context::Context[new_capacity];
    if (base_ != nullptr) {
        // vs2015 does not like this construct considering it unsafe:
        // - std::copy(base_, base_ + old_size, temp);
        // Ref.
        // https://stackoverflow.com/questions/12270224/xutility2227-warning-c4996-std-copy-impl
        for (size_t i = 0; i < (std::min)(old_size, new_capacity); i++) {
            temp[i] = base_[i];
        }
        delete[] base_;
    }
    base_ = temp;
    capacity_ = new_capacity;
}

BthreadLocalContextStorage::Stack::~Stack() noexcept {
    delete[] base_;
}

OPENTELEMETRY_API_SINGLETON BthreadLocalContextStorage::Stack&
BthreadLocalContextStorage::GetStack() {
    return *stack_;
}
} // namespace pain
