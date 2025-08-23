#pragma once

#include <asm-generic/errno.h>
#include <braft/raft.h>
#include <braft/route_table.h>
#include <braft/util.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <butil/endpoint.h>
#include <boost/assert.hpp>

#include <string>
#include <type_traits>

#include <pain/base/brpc_text_map_carrier.h>
#include <pain/base/future.h>
#include <pain/base/macro.h>
#include <pain/base/plog.h>
#include <pain/base/tracer.h>

namespace pain::deva {

constexpr int DEFAULT_TIMEOUT_MS = 20000;
constexpr int DEFAULT_CONNECT_TIMEOUT_MS = 2000;

template <typename T>
struct MemberWrapper {};

template <typename Ret, typename T>
struct MemberWrapper<Ret T::*> {
    using ClassType = T;
};

butil::Status init_channel(const char* group, const braft::PeerId& leader, ::brpc::Channel* channel);

template <typename CallFunc, typename Request, typename Response>
    requires std::is_member_function_pointer_v<CallFunc>
butil::Status call_rpc(const char* group,
                       const CallFunc& call_func,
                       const Request* request,
                       Response* response,
                       int timeout_ms = DEFAULT_TIMEOUT_MS,
                       int connect_timeout_ms = DEFAULT_CONNECT_TIMEOUT_MS) {
    BOOST_ASSERT(response != nullptr);
    PLOG_DEBUG(("desc", "rpc request")("request", request->DebugString()));

    SPAN("deva", span);

    braft::PeerId leader;

    while (true) {
        braft::PeerId leader;
        {
            SPAN("deva", select_leader_span, "select-leader");
            // Select leader of the target group from RouteTable
            if (braft::rtb::select_leader(group, &leader) != 0) {
                // Leader is unknown in RouteTable. Ask RouteTable to refresh leader
                // by sending RPCs.
                SPAN("deva", refresh_leader_span, "refresh-leader");
                butil::Status st = braft::rtb::refresh_leader(group, connect_timeout_ms);
                if (!st.ok()) {
                    // Not sure about the leader, sleep for a while and the ask again.
                    PLOG_WARN(("desc", "Fail to refresh leader")("error", st.error_str()));
                    refresh_leader_span->SetStatus(opentelemetry::trace::StatusCode::kError, st.error_str());
                    refresh_leader_span->End();
                    return st;
                }
                refresh_leader_span->SetStatus(opentelemetry::trace::StatusCode::kOk);
                refresh_leader_span->End();

                continue;
            }

            select_leader_span->SetStatus(opentelemetry::trace::StatusCode::kOk);
            select_leader_span->End();
        }

        ::brpc::Channel channel;
        auto status = init_channel(group, leader, &channel);
        if (!status.ok()) {
            PLOG_ERROR(("desc", "init channel failed")("error", status.error_str()));
            return status;
        }

        typename MemberWrapper<CallFunc>::ClassType::Stub stub(&channel);

        ::brpc::Controller cntl;
        cntl.set_timeout_ms(timeout_ms);

        std::string span_name = "issue-rpc";
        opentelemetry::trace::StartSpanOptions options;
        options.kind = opentelemetry::trace::SpanKind::kClient;
        SPAN("deva", span, "pain-rpc-call")

        auto current_ctx = opentelemetry::context::RuntimeContext::GetCurrent();
        BrpcTextMapCarrier carrier(&cntl);
        auto prop = opentelemetry::context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
        prop->Inject(carrier, current_ctx);
        // sync call
        std::invoke(call_func, &stub, &cntl, request, response, nullptr);

        if (cntl.Failed()) {
            response->Clear();
            PLOG_ERROR(("desc", "call rpc failed")("error", cntl.ErrorText()));
            braft::rtb::update_leader("deva", braft::PeerId());
            span->SetStatus(opentelemetry::trace::StatusCode::kError, cntl.ErrorText());
            span->End();
            return butil::Status(cntl.ErrorCode(), cntl.ErrorText());
        }

        if (response->header().status() != EREMCHG) {
            break;
        }

        if (response->header().message().empty()) {
            braft::rtb::update_leader("deva", braft::PeerId());
            const int64_t sleep_ms = 60000;
            PLOG_WARN(("desc", "redirect to unknown leader")("sleep_ms", sleep_ms));
            usleep(sleep_ms);
            continue;
        }

        auto& redirect = response->header().message();
        braft::rtb::update_leader("deva", braft::PeerId(redirect));
        span->AddEvent("redirect to " + redirect);
        PLOG_INFO(("desc", "redirect to leader")("leader", redirect));
    }

    PLOG_DEBUG(("desc", "rpc response")("response", response->DebugString()));
    return butil::Status::OK();
}

} // namespace pain::deva
