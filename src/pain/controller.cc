#include "pain/controller.h"
#include "pain/controller_impl.h"
#include "butil/iobuf.h"

namespace pain {

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

uint32_t Controller::error_code() const {
    return _impl->error_code();
}

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
