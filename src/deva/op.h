#pragma once
#include <atomic>
#include <cstdint>
#include <fmt/format.h>
#include <boost/intrusive_ptr.hpp>
#include "base/types.h"

namespace pain::deva {

enum class OpType {
    kInvalid = 0,
    // DevaOp: 1 ~ 100
    kOpen = 1,
    kClose = 2,
    kRemove = 3,
    kSeal = 4,
    kCreateChunk = 5,
    kRemoveChunk = 6,
    kSealChunk = 7,
    kSealAndNewChunk = 8,
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
    virtual void on_apply(int64_t index) = 0;
    virtual void on_finish(Status status) = 0;
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

class Rsm;
using RsmPtr = boost::intrusive_ptr<Rsm>;

void encode(OpPtr op, IOBuf* buf);
OpPtr decode(IOBuf* buf, std::move_only_function<OpPtr(OpType, IOBuf*)> decode);

} // namespace pain::deva

template <>
struct fmt::formatter<pain::deva::OpType> : public fmt::formatter<std::string_view> {
    template <typename FormatContext>
    auto format(pain::deva::OpType type, FormatContext& ctx) const {
        std::string_view name;
        switch (type) {
        case pain::deva::OpType::kOpen: name = "Open"; break;
        case pain::deva::OpType::kClose: name = "Close"; break;
        case pain::deva::OpType::kRemove: name = "Remove"; break;
        case pain::deva::OpType::kSeal: name = "Seal"; break;
        case pain::deva::OpType::kCreateChunk: name = "CreateChunk"; break;
        case pain::deva::OpType::kRemoveChunk: name = "RemoveChunk"; break;
        case pain::deva::OpType::kSealChunk: name = "SealChunk"; break;
        case pain::deva::OpType::kSealAndNewChunk: name = "SealAndNewChunk"; break;
        default: name = "Invalid"; break;
        }
        return fmt::formatter<std::string_view>::format(name, ctx);
    }
};
