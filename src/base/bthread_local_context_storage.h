#pragma once

#include <opentelemetry/context/runtime_context.h>

#include "base/bthread_local.h"
// NOLINTBEGIN

// this file style abide by the opentelemetry style guide
namespace pain {
class BthreadLocalContextStorage : public opentelemetry::context::RuntimeContextStorage {
public:
    BthreadLocalContextStorage() noexcept;
    ~BthreadLocalContextStorage() noexcept override;

    // Return the current context.
    opentelemetry::context::Context GetCurrent() noexcept override;

    // Resets the context to the value previous to the passed in token. This will
    // also detach all child contexts of the passed in token.
    // Returns true if successful, false otherwise.
    bool Detach(opentelemetry::context::Token& token) noexcept override;

    // Sets the current 'Context' object. Returns a token
    // that can be used to reset to the previous Context.
    std::unique_ptr<opentelemetry::context::Token>
    Attach(const opentelemetry::context::Context& context) noexcept override;

private:
    // A nested class to store the attached contexts in a stack.
    class Stack {
        friend class BthreadLocalContextStorage;
        friend class BthreadLocal<Stack>;
        Stack() noexcept;

        // Pops the top Context off the stack.
        void Pop() noexcept;

        bool Contains(const opentelemetry::context::Token& token) const noexcept;

        // Returns the Context at the top of the stack.
        opentelemetry::context::Context Top() const noexcept;

        // Pushes the passed in context pointer to the top of the stack
        // and resizes if necessary.
        void Push(const opentelemetry::context::Context& context) noexcept;

        // Reallocates the storage array to the pass in new capacity size.
        void Resize(size_t new_capacity) noexcept;

        ~Stack() noexcept;

        size_t size_;
        size_t capacity_;
        opentelemetry::context::Context* base_;
    };

    OPENTELEMETRY_API_SINGLETON Stack& GetStack();

    BthreadLocal<Stack> stack_;
};

} // namespace pain
// NOLINTEND
