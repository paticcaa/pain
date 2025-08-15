#include "deva/op.h"
#include <functional>
#include "butil/iobuf.h"
#include "butil/time.h"

namespace pain::deva {

void encode(OpPtr op, IOBuf* buf) {
    OpMeta op_meta = {};
    static_assert(sizeof(op_meta) == 64, "OpMeta size must be 64byte"); // NOLINT(readability-magic-numbers)
    op_meta.version = 1;
    op_meta.type = op->type();
    op_meta.timestamp = butil::gettimeofday_us();
    butil::IOBuf meta;
    op->encode(&meta);
    op_meta.size = meta.size();
    buf->append(&op_meta, sizeof(op_meta));
    buf->append(meta);
}

OpPtr decode(IOBuf* buf, std::move_only_function<OpPtr(OpType, IOBuf*)> decode) {
    OpMeta op_meta = {};
    static_assert(sizeof(op_meta) == 64, "OpMeta size must be 64byte"); // NOLINT(readability-magic-numbers)
    uint32_t meta_size = 0;
    buf->cutn(&op_meta, sizeof(op_meta));
    meta_size = op_meta.size;
    butil::IOBuf meta;
    buf->cutn(&meta, meta_size);
    auto op = decode(op_meta.type, &meta);
    return op;
}

} // namespace pain::deva
