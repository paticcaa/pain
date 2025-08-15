#pragma once
#include <butil/endpoint.h>
#include "base/uuid.h"

namespace pain {
enum class ChunkState {
    kInit = 0,
    kOpen = 1,
    kSealed = 2,
};

enum class ChunkType {
    kReplication = 0,
    kEc = 1, // not supported
};

struct Location {
    butil::EndPoint end_point;
};

struct SubChunk {
    UUID uuid;
    size_t size;
    Location location;
};

struct Chunk {
    uint64_t index;
    UUID uuid;
    ChunkState chunk_state;
    ChunkType chunk_type;
    uint32_t m; // replica count
    uint32_t n; // redundancy
    std::vector<SubChunk> sub_chunks;
};

} // namespace pain
