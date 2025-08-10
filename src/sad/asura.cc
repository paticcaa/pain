#include <brpc/channel.h>
#include <brpc/controller.h>
#include <butil/guid.h>
#include <butil/status.h>
#include <json2pb/pb_to_json.h>
#include <fstream>
#include <fmt/format.h>
#include <argparse/argparse.hpp>

#include "pain/proto/asura.pb.h"
#include "base/tracer.h"
#include "base/types.h"
#include "base/uuid.h"
#include "sad/common.h"
#include "sad/macro.h"

#define REGISTER_ASURA_CMD(cmd, ...) REGISTER(cmd, asura_parser, DEFER(__VA_ARGS__))

namespace pain::sad {
argparse::ArgumentParser& program();
}
namespace pain::sad::asura {
argparse::ArgumentParser asura_parser("asura", "1.0", argparse::default_arguments::none);

EXECUTE(program().add_subparser(asura_parser));
EXECUTE(asura_parser.add_description("send cmd to asura server")
            .add_argument("--host")
            .default_value(std::string("127.0.0.1:8201")));

static std::map<std::string, std::function<Status(argparse::ArgumentParser&)>> subcommands = {};

void add(const std::string& name, std::function<Status(argparse::ArgumentParser& parser)> func) {
    std::string name_;
    for (auto c : name) {
        if (c == '_') {
            c = '-';
        }
        name_ += c;
    }
    subcommands[name_] = func;
}

Status execute(argparse::ArgumentParser& parser) {
    for (const auto& [name, func] : subcommands) {
        if (parser.is_subcommand_used(name)) {
            SPAN(span, name);
            return func(parser.at<argparse::ArgumentParser>(name));
        }
    }
    std::cerr << parser;
    std::exit(1);
}

REGISTER_ASURA_CMD(register_deva, [](argparse::ArgumentParser& parser) {
    parser.add_description("add deva");
    parser.add_argument("--ip").required();
    parser.add_argument("--port").default_value(0u).scan<'i', uint32_t>().required();
});
COMMAND(register_deva) {
    SPAN(span);
    auto host = args.get<std::string>("--host");
    auto ip = args.get<std::string>("--ip");
    auto port = args.get<uint32_t>("--port");

    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.connect_timeout_ms = 2000;
    options.timeout_ms = 10000;
    options.max_retry = 0;
    if (channel.Init(host.c_str(), &options) != 0) {
        return Status(EAGAIN, "Fail to initialize channel");
    }

    brpc::Controller cntl;
    pain::proto::asura::RegisterDevaRequest request;
    pain::proto::asura::RegisterDevaResponse response;
    pain::proto::asura::TopologyService::Stub stub(&channel);
    pain::inject_tracer(&cntl);

    auto id = pain::UUID::generate();
    auto deva_server = request.add_deva_servers();
    deva_server->mutable_id()->set_low(id.low());
    deva_server->mutable_id()->set_high(id.high());
    deva_server->set_ip(ip);
    deva_server->set_port(port);
    stub.RegisterDeva(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        return Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    print(cntl, &response);
    return Status::OK();
}

REGISTER_ASURA_CMD(list_deva, [](argparse::ArgumentParser& parser) { parser.add_description("list deva"); });
COMMAND(list_deva) {
    SPAN(span);
    auto host = args.get<std::string>("--host");

    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.connect_timeout_ms = 2000;
    options.timeout_ms = 10000;
    options.max_retry = 0;
    if (channel.Init(host.c_str(), &options) != 0) {
        return Status(EAGAIN, "Fail to initialize channel");
    }

    brpc::Controller cntl;
    pain::proto::asura::ListDevaRequest request;
    pain::proto::asura::ListDevaResponse response;
    pain::proto::asura::TopologyService::Stub stub(&channel);
    pain::inject_tracer(&cntl);

    stub.ListDeva(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        return Status(cntl.ErrorCode(), cntl.ErrorText());
    }

    print(cntl, &response);
    return Status::OK();
}

REGISTER_ASURA_CMD(register_manusya, [](argparse::ArgumentParser& parser) {
    parser.add_description("add manusya");
    parser.add_argument("--ip").required();
    parser.add_argument("--port").default_value(0u).scan<'i', uint32_t>().required();
    parser.add_argument("--pool").required();
});
COMMAND(register_manusya) {
    SPAN(span);
    auto host = args.get<std::string>("--host");
    auto ip = args.get<std::string>("--ip");
    auto port = args.get<uint32_t>("--port");
    auto pool = args.get<std::string>("--pool");
    if (!UUID::valid(pool)) {
        return Status(EINVAL, "Invalid pool id");
    }
    auto pool_id = UUID::from_str_or_die(pool);

    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.connect_timeout_ms = 2000;
    options.timeout_ms = 10000;
    options.max_retry = 0;
    if (channel.Init(host.c_str(), &options) != 0) {
        return Status(EAGAIN, "Fail to initialize channel");
    }

    brpc::Controller cntl;
    pain::proto::asura::RegisterManusyaRequest request;
    pain::proto::asura::RegisterManusyaResponse response;
    pain::proto::asura::TopologyService::Stub stub(&channel);
    pain::inject_tracer(&cntl);

    auto id = pain::UUID::generate();
    auto manusya_server = request.add_manusya_servers();
    manusya_server->mutable_id()->set_low(id.low());
    manusya_server->mutable_id()->set_high(id.high());
    manusya_server->set_ip(ip);
    manusya_server->set_port(port);
    manusya_server->mutable_pool_id()->set_low(pool_id.low());
    manusya_server->mutable_pool_id()->set_high(pool_id.high());
    stub.RegisterManusya(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        return Status(cntl.ErrorCode(), cntl.ErrorText());
    }

    print(cntl, &response);
    return Status::OK();
}

REGISTER_ASURA_CMD(list_manusya, [](argparse::ArgumentParser& parser) { parser.add_description("list manusya"); });
COMMAND(list_manusya) {
    SPAN(span);
    auto host = args.get<std::string>("--host");

    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.connect_timeout_ms = 2000;
    options.timeout_ms = 10000;
    options.max_retry = 0;
    if (channel.Init(host.c_str(), &options) != 0) {
        return Status(EAGAIN, "Fail to initialize channel");
    }

    brpc::Controller cntl;
    pain::proto::asura::ListManusyaRequest request;
    pain::proto::asura::ListManusyaResponse response;
    pain::proto::asura::TopologyService::Stub stub(&channel);
    pain::inject_tracer(&cntl);

    stub.ListManusya(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        return Status(cntl.ErrorCode(), cntl.ErrorText());
    }

    print(cntl, &response);
    return Status::OK();
}

REGISTER_ASURA_CMD(create_pool, [](argparse::ArgumentParser& parser) {
    parser.add_description("create pool");
    parser.add_argument("--name").required();
});
COMMAND(create_pool) {
    SPAN(span);
    auto host = args.get<std::string>("--host");
    auto name = args.get<std::string>("--name");

    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.connect_timeout_ms = 2000;
    options.timeout_ms = 10000;
    options.max_retry = 0;
    if (channel.Init(host.c_str(), &options) != 0) {
        return Status(EAGAIN, "Fail to initialize channel");
    }

    brpc::Controller cntl;
    pain::proto::asura::CreatePoolRequest request;
    pain::proto::asura::CreatePoolResponse response;
    pain::proto::asura::TopologyService::Stub stub(&channel);
    pain::inject_tracer(&cntl);

    request.set_pool_name(name);
    stub.CreatePool(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        return Status(cntl.ErrorCode(), cntl.ErrorText());
    }

    print(cntl, &response);
    return Status::OK();
}

} // namespace pain::sad::asura
