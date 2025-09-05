#include "asura/asura_service_impl.h"
#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <pain/base/uuid.h>
#include <cerrno>
#include <fmt/format.h>

namespace pain::asura {

static const std::string_view ASURA_DEVA = "asura_deva";
static const std::string_view ASURA_MANUSYA = "asura_manusya";

void AsuraServiceImpl::RegisterDeva(::google::protobuf::RpcController* controller,
                                    [[maybe_unused]] const pain::proto::asura::RegisterDevaRequest* request,
                                    [[maybe_unused]] pain::proto::asura::RegisterDevaResponse* response,
                                    ::google::protobuf::Closure* done) { // NOLINT(readability-non-const-parameter)
    ASURA_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);

    for (const auto& deva_server : request->deva_servers()) {
        auto uuid = UUID(deva_server.id().high(), deva_server.id().low());
        auto result = response->add_results();
        result->mutable_id()->CopyFrom(deva_server.id());
        result->set_code(0);
        result->set_message("ok");

        if (_store->hexists(ASURA_DEVA, uuid.str())) {
            result->set_code(EEXIST);
            result->set_message(
                fmt::format("{} existed. ip:{}, port:{}", uuid.str(), deva_server.ip(), deva_server.port()));
            continue;
        }

        auto status = _store->hset(ASURA_DEVA, uuid.str(), deva_server.SerializeAsString());
        if (!status.ok()) {
            result->set_code(status.error_code());
            result->set_message(status.error_cstr());
        }
    }
}

void AsuraServiceImpl::ListDeva(::google::protobuf::RpcController* controller,
                                [[maybe_unused]] const pain::proto::asura::ListDevaRequest* request,
                                [[maybe_unused]] pain::proto::asura::ListDevaResponse* response,
                                ::google::protobuf::Closure* done) { // NOLINT(readability-non-const-parameter)
    ASURA_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);
    auto it = _store->hgetall(ASURA_DEVA);
    while (it->valid()) {
        auto deva = response->add_deva_servers();
        auto value = it->value();
        deva->ParseFromArray(value.data(), value.size());
        it->next();
    }
}

} // namespace pain::asura
