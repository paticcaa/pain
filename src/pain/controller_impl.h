#pragma once

#include <butil/iobuf.h>

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

    void set_error_code(uint32_t error_code) {
        _error_code = error_code;
    }

    uint32_t error_code() const {
        return _error_code;
    }

private:
    uint32_t _error_code = 0;
    int _timeout_us = 0;
    bool _direct_io = true;
    butil::IOBuf _request_attachment;
    butil::IOBuf _response_attachment;
};

} // namespace pain
