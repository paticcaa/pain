#include "common/rsm/op.h"
#include <pain/base/plog.h>
#include <functional>
#include "butil/iobuf.h"
#include "butil/time.h"

namespace pain::common {

void encode(int32_t version, OpPtr op, IOBuf* buf) {
    OpMeta op_meta = {};
    static_assert(sizeof(op_meta) == 64, "OpMeta size must be 64byte"); // NOLINT(readability-magic-numbers)
    op_meta.version = version;
    op_meta.type = op->type();
    op_meta.timestamp = butil::gettimeofday_us();
    butil::IOBuf meta;
    op->encode(&meta);
    op_meta.size = meta.size();
    buf->append(&op_meta, sizeof(op_meta));
    buf->append(meta);
}

OpPtr decode(IOBuf* buf, std::move_only_function<OpPtr(int32_t, uint32_t, IOBuf*)> decode) {
    OpMeta op_meta = {};
    static_assert(sizeof(op_meta) == 64, "OpMeta size must be 64byte"); // NOLINT(readability-magic-numbers)
    uint32_t op_size = 0;
    buf->cutn(&op_meta, sizeof(op_meta));
    op_size = op_meta.size;
    if (buf->size() < op_size) {
        PLOG_ERROR(("desc", "op size is too small") //
                   ("op_size", op_size)             //
                   ("buf_size", buf->size()));
        return nullptr;
    }
    auto op = decode(op_meta.version, op_meta.type, buf);
    return op;
}

} // namespace pain::common
