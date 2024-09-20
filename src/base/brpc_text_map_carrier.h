#pragma once

#include <brpc/controller.h>
#include <brpc/http_header.h>
#include <opentelemetry/context/propagation/text_map_propagator.h>

namespace pain {
class BrpcTextMapCarrier
    : public opentelemetry::context::propagation::TextMapCarrier {
public:
    BrpcTextMapCarrier(brpc::Controller *cntl) :
        _cntl(cntl) {}
    BrpcTextMapCarrier() = default;
    virtual std::string_view Get(std::string_view key) const noexcept override {
        auto protocol = _cntl->request_protocol();
        if (protocol == brpc::PROTOCOL_HTTP || protocol == brpc::PROTOCOL_H2) {
            auto &headers = _cntl->http_request();
            std::string key_to_compare = key.data();
            auto value = headers.GetHeader(key_to_compare);
            if (value != nullptr) {
                return *value;
            }
            return "";
        } else {
            auto user_fields = _cntl->request_user_fields();
            auto value = user_fields->seek(std::string(key));
            if (value != nullptr) {
                return *value;
            }
        }

        return "";
    }

    virtual void Set(std::string_view key,
                     std::string_view value) noexcept override {
        auto protocol = _cntl->request_protocol();
        if (protocol == brpc::PROTOCOL_HTTP || protocol == brpc::PROTOCOL_H2) {
            auto &headers = _cntl->http_request();
            headers.SetHeader(std::string(key), std::string((value)));
        } else {
            (*_cntl->request_user_fields())[std::string(key)] = std::string(value);
        }
    }

    brpc::Controller *_cntl;
};
} // namespace pain
