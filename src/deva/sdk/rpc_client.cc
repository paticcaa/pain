#include "deva/sdk/rpc_client.h"

#include <brpc/channel.h>

namespace pain::deva {

butil::Status init_channel(const char* group, const braft::PeerId& leader, ::brpc::Channel* channel) {
    if (channel == nullptr) {
        return butil::Status(EINVAL, "Channel is nullptr");
    }
    const int default_timeout_ms = 20000;
    const int default_connect_timeout_ms = 2000;
    SPAN("deva", init_channel_span, "init-channel");
    ::brpc::ChannelOptions opts;
    opts.timeout_ms = default_timeout_ms;
    opts.max_retry = 0;
    opts.connect_timeout_ms = default_connect_timeout_ms;

    if (channel->Init(leader.addr, &opts) != 0) {
        PLOG_ERROR(("desc", "Fail to init channel to")("leader", leader.to_string()));
        braft::rtb::update_leader(group, braft::PeerId());
        init_channel_span->SetStatus(opentelemetry::trace::StatusCode::kError, "init channel error");
        init_channel_span->End();
        return butil::Status(ECONNREFUSED, "Fail to init channel to " + leader.to_string());
    }

    init_channel_span->SetStatus(opentelemetry::trace::StatusCode::kOk);
    init_channel_span->End();
    return butil::Status::OK();
}

} // namespace pain::deva
