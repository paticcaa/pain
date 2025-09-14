#pragma once

#include "pain/proto/deva_store.pb.h"
#include "common/rsm/container_op.h"
#include "common/rsm/op_factory.h"

#define BRANCH(name)                                                                                                   \
    case OpType::k##name:                                                                                              \
        return create_op<OpType::k##name, proto::deva::store::name##Request, proto::deva::store::name##Response>(      \
            version, rsm);                                                                                             \
        break;

namespace pain::deva {
class Deva;

using OpPtr = common::OpPtr;
using RsmPtr = common::RsmPtr;

template <typename Request, typename Response>
using ContainerOp = common::ContainerOp<Deva, Request, Response>;

enum class OpType : uint32_t {
    kInvalid = 0,
    // DevaOp: 1 ~ 100
    DEFINE_RSM_OP(1, CreateFile, true),
    DEFINE_RSM_OP(2, CreateDir, true),
    DEFINE_RSM_OP(3, RemoveFile, true),
    DEFINE_RSM_OP(4, SealFile, true),
    DEFINE_RSM_OP(5, CreateChunk, true),
    DEFINE_RSM_OP(6, CheckInChunk, true),
    DEFINE_RSM_OP(7, SealChunk, true),
    DEFINE_RSM_OP(8, SealAndNewChunk, true),
    DEFINE_RSM_OP(9, ReadDir, false),
    DEFINE_RSM_OP(10, GetFileInfo, false),
    DEFINE_RSM_OP(20, ManusyaHeartbeat, false),
    DEFINE_RSM_OP(21, ListManusya, false),
    DEFINE_RSM_OP(100, MaxDevaOp, true),
};

template <OpType OpType, typename Request, typename Response>
OpPtr create_op(int32_t version, RsmPtr rsm) {
    Request request;
    OpPtr op = nullptr;

    if constexpr (OpType < OpType::kMaxDevaOp) {
        op = new ContainerOp<Request, Response>(version, static_cast<uint32_t>(OpType), rsm, request, nullptr);
    }
    return op;
}

class DevaOpFactory : public common::OpFactory {
public:
    OpPtr create(uint32_t op_type, int32_t version, RsmPtr rsm) override {
        switch (static_cast<OpType>(op_type)) {
            BRANCH(CreateFile)
            BRANCH(CreateDir)
            BRANCH(ReadDir)
            BRANCH(RemoveFile)
            BRANCH(SealFile)
            BRANCH(CreateChunk)
            BRANCH(CheckInChunk)
            BRANCH(SealChunk)
            BRANCH(SealAndNewChunk)
        default:
            BOOST_ASSERT_MSG(false, fmt::format("unknown op type: {}", op_type).c_str());
        }
        return nullptr;
    }
};

} // namespace pain::deva
