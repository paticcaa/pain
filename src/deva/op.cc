#include "deva/op.h"
#include "butil/iobuf.h"
#include "butil/time.h"
#include "deva/bridge.h"

namespace pain::deva {

void encode(OpPtr op, IOBuf* buf) {
    OpMeta op_meta = {};
    static_assert(sizeof(op_meta) == 64, "OpMeta size must be 64byte");
    op_meta.version = 1;
    op_meta.type = op->type();
    op_meta.timestamp = butil::gettimeofday_us();
    butil::IOBuf meta;
    op->encode(&meta);
    op_meta.size = meta.size();
    buf->append(&op_meta, sizeof(op_meta));
    buf->append(meta);
}

OpPtr decode(IOBuf* buf, RsmPtr rsm) {
    OpMeta op_meta = {};
    static_assert(sizeof(op_meta) == 64, "OpMeta size must be 64byte");
    uint32_t meta_size = 0;
    buf->cutn(&op_meta, sizeof(op_meta));
    meta_size = op_meta.size;
    butil::IOBuf meta;
    buf->cutn(&meta, meta_size);
    auto op = decode(op_meta.type, &meta, rsm);
    return op;
}

} // namespace pain::deva
