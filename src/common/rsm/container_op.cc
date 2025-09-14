#include "common/rsm/container_op.h"
#include "common/rsm/op_factory.h"

namespace pain::common {

OpPtr decode(int32_t version, uint32_t op_type, IOBuf* buf, RsmPtr rsm) {
    auto op_factory = rsm->container()->op_factory();
    auto op = op_factory->create(op_type, version, rsm);
    op->decode(buf);
    return op;
}

} // namespace pain::common
