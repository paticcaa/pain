#include "asura/topology_service_impl.h"
#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <cerrno>
#include <fmt/format.h>
#include "base/uuid.h"

namespace pain::asura {

static const std::string_view kAsuraPool = "asura_pool";
static const std::string_view kAsuraDeva = "asura_deva";
static const std::string_view kAsuraManusya = "asura_manusya";

void TopologyServiceImpl::CreatePool(::google::protobuf::RpcController* controller,
                                     const pain::core::asura::CreatePoolRequest* request,
                                     pain::core::asura::CreatePoolResponse* response,
                                     ::google::protobuf::Closure* done) {
    ASURA_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);
    pain::core::asura::Pool pool;
    if (_store->hexists(kAsuraPool, request->pool_name())) {
        cntl->SetFailed(EEXIST, "pool %s already exists", request->pool_name().c_str());
        return;
    }

    static thread_local UUIDv4::UUIDGenerator<std::mt19937_64> uuid_gen;
    auto uuid = UUID(uuid_gen.getUUID());
    pool.set_name(request->pool_name());
    pool.mutable_uuid()->set_high(uuid.high());
    pool.mutable_uuid()->set_low(uuid.low());
    pool.mutable_placement_policy()->CopyFrom(request->placement_policy());
    _store->hset("pool", request->pool_name(), pool.SerializeAsString());
}

void TopologyServiceImpl::ListPool(::google::protobuf::RpcController* controller,
                                   const pain::core::asura::ListPoolRequest* request,
                                   pain::core::asura::ListPoolResponse* response,
                                   ::google::protobuf::Closure* done) {
    ASURA_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);
    auto it = _store->hgetall(kAsuraPool);
    while (it->valid()) {
        auto pool = response->add_pools();
        auto value = it->value();
        pool->ParseFromArray(value.data(), value.size());
        it->next();
    }
}

void TopologyServiceImpl::RegisterDeva(::google::protobuf::RpcController* controller,
                                       const pain::core::asura::RegisterDevaRequest* request,
                                       pain::core::asura::RegisterDevaResponse* response,
                                       ::google::protobuf::Closure* done) {
    ASURA_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);

    for (const auto& deva_server : request->deva_servers()) {
        auto uuid = UUID(deva_server.id().high(), deva_server.id().low());
        auto result = response->add_results();
        result->mutable_id()->CopyFrom(deva_server.id());
        result->set_code(0);
        result->set_message("ok");

        if (_store->hexists(kAsuraDeva, uuid.str())) {
            result->set_code(EEXIST);
            result->set_message(
                fmt::format("{} existed. ip:{}, port:{}", uuid.str(), deva_server.ip(), deva_server.port()));
            continue;
        }

        auto status = _store->hset(kAsuraDeva, uuid.str(), deva_server.SerializeAsString());
        if (!status.ok()) {
            result->set_code(status.error_code());
            result->set_message(status.error_cstr());
        }
    }
}

void TopologyServiceImpl::RegisterManusya(::google::protobuf::RpcController* controller,
                                          const pain::core::asura::RegisterManusyaRequest* request,
                                          pain::core::asura::RegisterManusyaResponse* response,
                                          ::google::protobuf::Closure* done) {
    ASURA_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);
    for (const auto& manusya_server : request->manusya_servers()) {
        auto uuid = UUID(manusya_server.id().high(), manusya_server.id().low());
        auto result = response->add_results();
        result->mutable_id()->CopyFrom(manusya_server.id());
        result->set_code(0);
        result->set_message("ok");

        if (!_store->hexists(kAsuraManusya, uuid.str())) {
            result->set_code(EEXIST);
            result->set_message(
                fmt::format("{} existed. ip:{}, port:{}", uuid.str(), manusya_server.ip(), manusya_server.port()));
            continue;
        }

        auto status = _store->hset(kAsuraManusya, uuid.str(), manusya_server.SerializeAsString());
        if (!status.ok()) {
            result->set_code(status.error_code());
            result->set_message(status.error_cstr());
        }
    }
}

void TopologyServiceImpl::ListDeva(::google::protobuf::RpcController* controller,
                                   const pain::core::asura::ListDevaRequest* request,
                                   pain::core::asura::ListDevaResponse* response,
                                   ::google::protobuf::Closure* done) {
    ASURA_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);
    auto it = _store->hgetall(kAsuraDeva);
    while (it->valid()) {
        auto deva = response->add_deva_servers();
        auto value = it->value();
        deva->ParseFromArray(value.data(), value.size());
        it->next();
    }
}

void TopologyServiceImpl::ListManusya(::google::protobuf::RpcController* controller,
                                      const pain::core::asura::ListManusyaRequest* request,
                                      pain::core::asura::ListManusyaResponse* response,
                                      ::google::protobuf::Closure* done) {
    ASURA_SPAN(span, controller);
    brpc::ClosureGuard done_guard(done);
    auto it = _store->hgetall(kAsuraManusya);
    while (it->valid()) {
        auto manusya = response->add_manusya_servers();
        auto value = it->value();
        manusya->ParseFromArray(value.data(), value.size());
        it->next();
    }
}

} // namespace pain::asura
