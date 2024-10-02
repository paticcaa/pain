#pragma once

#include <braft/raft.h>
#include <functional>
#include "base/future.h"
#include "base/plog.h"
#include "base/types.h"
#include "deva/op.h"
#include "deva/rsm.h"

namespace pain::deva {

class OpClosure : public braft::Closure {
public:
    OpClosure(OpPtr op) : _op(op) {}

    void Run() {
        std::unique_ptr<OpClosure> guard(this);
        if (status().ok()) {
            _op->on_apply(_index);
            return;
        }
        _op->on_finish(status());
    }

    void set_index(int64_t index) {
        _index = index;
    }

private:
    int64_t _index = 0;
    OpPtr _op;
};

OpPtr decode(OpType op_type, IOBuf* buf, RsmPtr rsm);

template <typename Request, typename Response>
class DevaOp : public Op {
public:
    using OnFinish = std::move_only_function<void(Status)>;
    DevaOp(OpType type, RsmPtr rsm, Request request, Response* response = nullptr, OnFinish finish = nullptr) :
        _type(type),
        _rsm(rsm),
        _request(std::move(request)),
        _response(response),
        _finish(std::move(finish)) {
        if (response == nullptr) {
            _response = &_internal_response;
        }
    }

    OpType type() const override {
        return _type;
    }

    void apply() override {
        braft::Task task;
        IOBuf buf;
        OpPtr self(this);
        pain::deva::encode(self, &buf);
        task.data = &buf;
        task.done = new OpClosure(self);
        task.expected_term = -1;
        _rsm->apply(task);
        PLOG_DEBUG(("desc", "apply op")("type", _type));
    }

    void on_apply(int64_t index) override {
        PLOG_DEBUG(("desc", "on apply op")("type", _type)("index", index));
        auto status = _rsm->deva()->process(&_request, _response, index);
        on_finish(std::move(status));
    }

    void encode(IOBuf* buf) override {
        butil::IOBufAsZeroCopyOutputStream wrapper(buf);
        if (!_request.SerializeToZeroCopyStream(&wrapper)) {
            BOOST_ASSERT_MSG(false, "serialize response failed");
        }
    }

    void decode(IOBuf* buf) override {
        butil::IOBufAsZeroCopyInputStream wrapper(*buf);
        if (!_request.ParseFromZeroCopyStream(&wrapper)) {
            BOOST_ASSERT_MSG(false, "parse request failed");
        }
    }

    void on_finish(Status status) override {
        PLOG_DEBUG(("desc", "on finish op")("type", _type)("status", status.error_str()));
        if (_finish) {
            _finish(std::move(status));
        }
    }

private:
    OpType _type;
    OnFinish _finish;
    RsmPtr _rsm;
    Request _request;
    Response* _response;
    Response _internal_response;
};

template <OpType OpType, typename Request, typename Response>
void bridge(const Request& request, Response* response, std::move_only_function<void(Status)> cb) {
    // TODO: get rsm by pool id and partition id
    auto rsm = default_rsm();
    (new DevaOp<Request, Response>(OpType, rsm, request, response, [cb = std::move(cb)](Status status) mutable {
        cb(std::move(status));
    }))->apply();
}

// Future style
template <OpType OpType, typename Request, typename Response>
Future<Status> bridge(const Request& request, Response* response) {
    Promise<Status> promise;
    auto future = promise.get_future();
    bridge<OpType>(request, response, [promise = std::move(promise)](Status status) mutable {
        promise.set_value(std::move(status));
    });
    return future;
}

} // namespace pain::deva
