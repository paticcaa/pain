#include "deva/container_op.h"
#include <pain/base/types.h>
#include <boost/assert.hpp>
#include "pain/proto/deva_store.pb.h"
#include "deva/deva.h"

namespace pain::deva {

template <OpType OpType, typename Request, typename Response>
OpPtr create(RsmPtr rsm) {
    Request request;
    OpPtr op = nullptr;

    if constexpr (static_cast<int>(OpType) < 100) { // NOLINT(readability-magic-numbers)
        op = new ContainerOp<Deva, Request, Response>(OpType, rsm, request, nullptr);
    }
    return op;
}

#define BRANCH(name)                                                                                                   \
    case OpType::k##name:                                                                                              \
        return create<OpType::k##name, proto::deva::store::name##Request, proto::deva::store::name##Response>(rsm);    \
        break;

OpPtr decode(OpType op_type, IOBuf* buf, RsmPtr rsm) {
    auto op = [](OpType op_type, IOBuf* buf, RsmPtr rsm) -> OpPtr {
        switch (op_type) {
            BRANCH(CreateFile)
            BRANCH(RemoveFile)
            BRANCH(SealFile)
            BRANCH(CreateChunk)
            BRANCH(CheckInChunk)
            BRANCH(SealChunk)
            BRANCH(SealAndNewChunk)
        default:
            BOOST_ASSERT_MSG(false, "unknown op type");
        }
        return nullptr;
    }(op_type, buf, rsm);
    op->decode(buf);
    return op;
}

#undef BRANCH

} // namespace pain::deva
