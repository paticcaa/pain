#pragma once

#include <google/protobuf/service.h>

namespace pain {

class Controller : public google::protobuf::RpcController {
public:
    void Reset() override {}
    bool Failed() const override {
        return false;
    }
    std::string ErrorText() const override {
        return "";
    }
    void StartCancel() override {}
    void SetFailed(const std::string& reason) override {}
    bool IsCanceled() const override {
        return false;
    }
    void NotifyOnCancel(google::protobuf::Closure* callback) override {}

    void set_timeout_us(int timeout_us) {
        _timeout_us = timeout_us;
    }

    int timeout_us() const {
        return _timeout_us;
    }

    void set_direct_io(bool direct_io) {
        _direct_io = direct_io;
    }

    bool direct_io() const {
        return _direct_io;
    }

private:
    int _timeout_us = 1000000;
    bool _direct_io = false;
};

} // namespace pain
