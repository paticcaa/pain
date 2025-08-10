#pragma once

#include <butil/iobuf.h>
#include <google/protobuf/service.h>

namespace pain {

class ControllerImpl;
class Controller : public google::protobuf::RpcController {
public:
    Controller();
    virtual ~Controller();
    void Reset() override;
    bool Failed() const override;
    std::string ErrorText() const override;
    void StartCancel() override;
    void SetFailed(const std::string& reason) override;
    bool IsCanceled() const override;
    void NotifyOnCancel(google::protobuf::Closure* callback) override;
    uint32_t error_code() const;
    void set_timeout_us(int timeout_us);
    int timeout_us() const;
    void set_direct_io(bool direct_io);
    bool direct_io() const;
    butil::IOBuf& request_attachment();
    butil::IOBuf& response_attachment();

private:
    ControllerImpl* _impl;
};

} // namespace pain
