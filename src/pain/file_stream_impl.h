#pragma once
#include <list>

#include <pain/base/uuid.h>
#include "pain/chunk.h"
#include "pain/proto/common.pb.h"
#include "pain/proto/pain.pb.h"

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
    FILE_STREAM_METHOD(Append);
    FILE_STREAM_METHOD(Read);

private:
    friend class FileSystem;
    ~FileStreamImpl() override = default;
    proto::FileInfo _file_info;
    std::string _file_id;
    friend class FileStream;
};

} // namespace pain

#undef FILE_STREAM_METHOD
