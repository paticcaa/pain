#pragma once

#include <braft/raft.h>
#include <pain/base/macro.h>
#include <pain/base/plog.h>
#include <pain/base/types.h>
#include <functional>
#include "common/rsm/op.h"
#include "common/rsm/rsm.h"

namespace pain::common {

class OpClosure : public braft::Closure {
public:
    OpClosure(OpPtr op, std::shared_ptr<opentelemetry::trace::Span> span) : _op(op), _span(span) {}

    void Run() override {
        opentelemetry::trace::Scope scope(_span);
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
    std::shared_ptr<opentelemetry::trace::Span> _span;
};

OpPtr decode(int32_t version, uint32_t op_type, IOBuf* buf, RsmPtr rsm);

template <typename ContainerType, typename Request, typename Response>
class ContainerOp : public Op {
public:
    using OnFinish = std::move_only_function<void(Status)>;
    ContainerOp(int32_t version,
                uint32_t type,
                RsmPtr rsm,
                Request request,
                Response* response = nullptr,
                OnFinish finish = nullptr) :
        _version(version),
        _type(type),
        _rsm(rsm),
        _request(std::move(request)),
        _response(response),
        _finish(std::move(finish)) {
        if (response == nullptr) {
            _response = &_internal_response;
        }
    }

    uint32_t type() const override {
        return _type;
    }

    void apply() override {
        SPAN("common", span);
        PLOG_DEBUG(("desc", "apply op")("type", _type)("version", _version));
        if (need_apply(_type)) {
            braft::Task task;
            IOBuf buf;
            OpPtr self(this);
            pain::common::encode(_version, self, &buf);
            task.data = &buf;
            task.done = new OpClosure(self, span);
            task.expected_term = -1;
            _rsm->apply(task);
        } else {
            on_apply(0);
        }
    }

    void on_apply(int64_t index) override {
        PLOG_DEBUG(("desc", "on apply op")("type", _type)("index", index));
        auto container = _rsm->container();
        auto c = static_cast<ContainerType*>(container.get());
        auto status = c->process(_version, &_request, _response, index);
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

protected:
    int32_t _version;
    uint32_t _type;
    RsmPtr _rsm;
    Request _request;
    Response* _response;
    Response _internal_response;
    OnFinish _finish;
};

} // namespace pain::common
