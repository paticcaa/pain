#pragma once
#include <atomic>
#include <cstdint>
#include <boost/intrusive_ptr.hpp>
#include "base/types.h"

namespace pain::deva {

enum class OpType {
    INVALIDE = 0,
    OPEN = 1,
    CLOSE = 2,
    REMOVE = 3,
    SEAL = 4,
    CREATE_CHUNK = 5,
    REMOVE_CHUNK = 6,
    SEAL_CHUNK = 7,
    SEAL_AND_NEW_CHUNK = 8,
};

struct OpMeta {
    int32_t version; // op version
    OpType type;
    uint64_t timestamp;
    uint32_t size;
    char reserved[40];
};

static_assert(sizeof(OpMeta) == 64, "OpMeta size must be 64byte");

class Op;
using OpPtr = boost::intrusive_ptr<Op>;
class Op {
public:
    Op() = default;
    virtual ~Op() = default;
    virtual OpType type() const = 0;
    virtual void apply() = 0;
    virtual void on_apply() = 0;
    virtual void encode(IOBuf* buf) = 0;
    virtual void decode(IOBuf* buf) = 0;

private:
    std::atomic<int> _use_count = 0;
    friend void intrusive_ptr_add_ref(Op* op) {
        ++op->_use_count;
    }
    friend void intrusive_ptr_release(Op* op) {
        if (op->_use_count.fetch_sub(1) == 1) {
            delete op;
        }
    }
};

void encode(OpPtr op, IOBuf* buf);
OpPtr decode(IOBuf* buf);

} // namespace pain::deva
