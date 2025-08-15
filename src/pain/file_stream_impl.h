#pragma once
#include <list>

#include "pain/chunk.h"
#include "pain/proto/pain.pb.h"
#include "base/uuid.h"

#define FILE_STREAM_METHOD(name)                                                                                       \
    void name(::google::protobuf::RpcController* controller,                                                           \
              const ::pain::proto::name##Request* request,                                                             \
              ::pain::proto::name##Response* response,                                                                 \
              ::google::protobuf::Closure* done) override

namespace pain {

class Controller;

// NOLINTNEXTLINE(cppcoreguidelines-virtual-class-destructor)
class FileStreamImpl : public pain::proto::FileService {
public:
    FILE_STREAM_METHOD(Open);
    FILE_STREAM_METHOD(Append);
    FILE_STREAM_METHOD(Read);
    FILE_STREAM_METHOD(Seal);
    FILE_STREAM_METHOD(Close);

private:
    ~FileStreamImpl() override = default;
    UUID _uuid;
    std::list<Chunk> _chunks;
    friend class FileStream;
};

} // namespace pain

#undef FILE_STREAM_METHOD
