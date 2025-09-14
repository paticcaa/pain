#pragma once

#include "common/rsm/op.h"

namespace pain::common {

class OpFactory {
public:
    OpFactory() = default;
    virtual ~OpFactory() = default;
    virtual OpPtr create(uint32_t op_type, int32_t version, RsmPtr rsm) = 0;
};

} // namespace pain::common
