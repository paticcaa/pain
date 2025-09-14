#pragma once

#include <braft/raft.h>
#include <pain/base/future.h>
#include <pain/base/types.h>
#include <functional>
#include "common/rsm/container_op.h"
#include "common/rsm/op.h"
#include "common/rsm/rsm.h"

namespace pain::common {

template <typename ContainerType, typename Request, typename Response>
void bridge(uint32_t op_type,
            int32_t version,
            common::RsmPtr rsm,
            const Request& request,
            Response* response,
            std::move_only_function<void(Status)> cb) {
    auto op = new common::ContainerOp<ContainerType, Request, Response>(
        version, op_type, rsm, request, response, [cb = std::move(cb)](Status status) mutable {
            cb(std::move(status));
        });
    op->apply();
}

// Future style
template <typename ContainerType, typename Request, typename Response>
Future<Status> bridge(uint32_t op_type, int32_t version, RsmPtr rsm, const Request& request, Response* response) {
    Promise<Status> promise;
    auto future = promise.get_future();
    bridge<ContainerType>(
        op_type, version, rsm, request, response, [promise = std::move(promise)](Status status) mutable {
            promise.set_value(std::move(status));
        });
    return future;
}

} // namespace pain::common
