#pragma once

#include "common/rsm/bridge.h"
#include "deva/deva_op_factory.h"

namespace pain::deva {

template <typename ContainerType, OpType OpType, typename Request, typename Response>
void bridge(int32_t version,
            common::RsmPtr rsm,
            const Request& request,
            Response* response,
            std::move_only_function<void(Status)> cb) {
    common::bridge<ContainerType>(static_cast<uint32_t>(OpType), version, rsm, request, response, std::move(cb));
}

// Future style
template <typename ContainerType, OpType OpType, typename Request, typename Response>
Future<Status> bridge(int32_t version, common::RsmPtr rsm, const Request& request, Response* response) {
    return common::bridge<ContainerType>(static_cast<uint32_t>(OpType), version, rsm, request, response);
}

} // namespace pain::deva
