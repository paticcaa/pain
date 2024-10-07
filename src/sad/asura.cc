#include <brpc/channel.h>
#include <brpc/controller.h>
#include <butil/guid.h>
#include <butil/status.h>
#include <json2pb/pb_to_json.h>
#include <fstream>
#include <fmt/format.h>
#include <argparse/argparse.hpp>

#include "pain/core/asura.pb.h"
#include "base/tracer.h"
#include "base/types.h"
#include "base/uuid.h"
#include "sad/common.h"
#include "sad/macro.h"

#define ASURA_CMD(cmd) REGISTER(cmd, asura_parser)

namespace pain::sad {
argparse::ArgumentParser& program();
}
namespace pain::sad::asura {
argparse::ArgumentParser asura_parser("asura", "1.0", argparse::default_arguments::none);

RUN(program().add_subparser(asura_parser));
RUN(asura_parser.add_description("send cmd to asura server")
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

ASURA_CMD(register_deva)
RUN(ARGS(register_deva).add_description("add deva"))
RUN(ARGS(register_deva).add_argument("--ip").required())
RUN(ARGS(register_deva).add_argument("--port").default_value(0u).scan<'i', uint32_t>().required())
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
    pain::core::asura::RegisterDevaRequest request;
    pain::core::asura::RegisterDevaResponse response;
    pain::core::asura::TopologyService::Stub stub(&channel);
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

} // namespace pain::sad::asura
