#include "deva/container_op.h"
#include <boost/assert.hpp>
#include "pain/core/deva_store.pb.h"
#include "base/types.h"
#include "deva/deva.h"

namespace pain::deva {

template <OpType OpType, typename Request, typename Response>
OpPtr create(RsmPtr rsm) {
    Request request;
    OpPtr op = nullptr;

    if constexpr (static_cast<int>(OpType) < 100) {
        op = new ContainerOp<Deva, Request, Response>(OpType, rsm, request, nullptr);
    }
    return op;
}

#define BRANCH(name)                                                                                                   \
    case OpType::k##name:                                                                                              \
        return create<OpType::k##name, core::deva::store::name##Request, core::deva::store::name##Response>(rsm);      \
        break;

OpPtr decode(OpType op_type, IOBuf* buf, RsmPtr rsm) {
    auto op = [](OpType op_type, IOBuf* buf, RsmPtr rsm) -> OpPtr {
        switch (op_type) {
            BRANCH(Open)
            BRANCH(Close)
            BRANCH(Remove)
            BRANCH(Seal)
            BRANCH(CreateChunk)
            BRANCH(RemoveChunk)
            BRANCH(SealChunk)
            BRANCH(SealAndNewChunk)
        default: BOOST_ASSERT_MSG(false, "unknown op type");
        }
        return nullptr;
    }(op_type, buf, rsm);
    op->decode(buf);
    return op;
}

#undef BRANCH

} // namespace pain::deva
