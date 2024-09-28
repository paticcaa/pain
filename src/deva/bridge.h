#pragma once

#include <braft/raft.h>
#include "base/types.h"
#include "deva/op.h"
#include "deva/rsm.h"
#include "google/protobuf/message.h"
#include "google/protobuf/service.h"
#include "google/protobuf/stubs/callback.h"

namespace pain::deva {

class OpClosure : public braft::Closure {
public:
    OpClosure(OpPtr op) : _op(op) {}

    void Run() {
        _op->on_apply();
        delete this;
    }

private:
    OpPtr _op;
};

template <typename Request, typename Response>
class BaseOp : public Op {
public:
    using Task = std::move_only_function<void(const Request*, Response*)>;
    BaseOp(OpType type,
           Task task,
           RsmPtr rsm,
           ::google::protobuf::RpcController* controller = nullptr,
           const google::protobuf::Message* request = nullptr,
           google::protobuf::Message* response = nullptr,
           google::protobuf::Closure* done = nullptr) :
        _type(type),
        _task(std::move(task)),
        _rsm(rsm),
        _controller(controller),
        _request(request),
        _response(response),
        _done(done) {
        if (_request == nullptr) {
            _request = &_internal_request;
        }
        if (_response == nullptr) {
            _response = &_internal_response;
        }
    }

    OpType type() const override {
        return _type;
    }

    void apply() override {
        braft::Task task;
        IOBuf buf;
        encode(&buf);
        task.data = &buf;
        task.done = new OpClosure(this);
        task.expected_term = -1;
        _rsm->apply(task);
    }

    void on_apply() override {
        _task(static_cast<const Request*>(_request), static_cast<Response*>(_response));
        if (_done) {
            _done->Run();
        }
    }

    void encode(IOBuf* buf) override {
        butil::IOBufAsZeroCopyOutputStream wrapper(buf);
        if (!_request->SerializeToZeroCopyStream(&wrapper)) {
            BOOST_ASSERT_MSG(false, "serialize response failed");
        }
    }

    void decode(IOBuf* buf) override {
        butil::IOBufAsZeroCopyInputStream wrapper(*buf);
        if (!_internal_request.ParseFromZeroCopyStream(&wrapper)) {
            BOOST_ASSERT_MSG(false, "parse request failed");
        }
    }

private:
    OpType _type;
    Task _task;
    RsmPtr _rsm;
    ::google::protobuf::RpcController* _controller;
    const google::protobuf::Message* _request;
    google::protobuf::Message* _response;
    google::protobuf::Closure* _done;

    Request _internal_request;
    Response _internal_response;
};

template <OpType OpType, typename Request, typename Response>
void bridge(google::protobuf::RpcController* controller,
            const Request* request,
            Response* response,
            google::protobuf::Closure* done) {
    auto rsm = default_rsm();
    auto op = new BaseOp<Request, Response>(
        OpType, [](auto* request, auto* response) {}, rsm, controller, request, response, done);
    op->apply();
}

} // namespace pain::deva
