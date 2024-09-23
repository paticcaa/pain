#include "pain/controller.h"
#include "butil/iobuf.h"

namespace pain {

class ControllerImpl {
public:
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
    butil::IOBuf& request_attachment() {
        return _request_attachment;
    }
    butil::IOBuf& response_attachment() {
        return _response_attachment;
    }

private:
    int _timeout_us = 0;
    bool _direct_io = true;
    butil::IOBuf _request_attachment;
    butil::IOBuf _response_attachment;
};

Controller::Controller() : _impl(new ControllerImpl) {}

Controller::~Controller() {
    delete _impl;
}

void Controller::Reset() {}
bool Controller::Failed() const {
    return false;
}
std::string Controller::ErrorText() const {
    return "";
}

void Controller::StartCancel() {}

void Controller::SetFailed(const std::string& reason) {}
bool Controller::IsCanceled() const {
    return false;
}
void Controller::NotifyOnCancel(google::protobuf::Closure* callback) {}

void Controller::set_timeout_us(int timeout_us) {
    _impl->set_timeout_us(timeout_us);
}

int Controller::timeout_us() const {
    return _impl->timeout_us();
}

void Controller::set_direct_io(bool direct_io) {
    _impl->set_direct_io(direct_io);
}

bool Controller::direct_io() const {
    return _impl->direct_io();
}

butil::IOBuf& Controller::request_attachment() {
    return _impl->request_attachment();
}

butil::IOBuf& Controller::response_attachment() {
    return _impl->response_attachment();
}

} // namespace pain
