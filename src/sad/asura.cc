#include <brpc/channel.h>
#include <brpc/controller.h>
#include <butil/guid.h>
#include <butil/status.h>
#include <json2pb/pb_to_json.h>
#include <fstream>
#include <fmt/format.h>
#include <argparse/argparse.hpp>

#include <pain/base/tracer.h>
#include <pain/base/types.h>
#include <pain/base/uuid.h>
#include "pain/proto/asura.pb.h"
#include "sad/common.h"
#include "sad/macro.h"

#define REGISTER_ASURA_CMD(cmd, ...) REGISTER(cmd, asura_parser, DEFER(__VA_ARGS__))

namespace pain::sad {
argparse::ArgumentParser& program();
}
namespace pain::sad::asura {
// NOLINTNEXTLINE
argparse::ArgumentParser asura_parser("asura", "1.0", argparse::default_arguments::none);

EXECUTE(program().add_subparser(asura_parser));
EXECUTE(asura_parser.add_description("send cmd to asura server")
            .add_argument("--host")
            .default_value(std::string("127.0.0.1:8201")));

// NOLINTNEXTLINE
static std::map<std::string, std::function<Status(argparse::ArgumentParser&)>> subcommands = {};

void add(const std::string& name, std::function<Status(argparse::ArgumentParser& parser)> func) {
    std::string normalized_name;
    for (auto c : name) {
        if (c == '_') {
            c = '-';
        }
        normalized_name += c;
    }
    subcommands[normalized_name] = func;
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
    // NOLINTNEXTLINE
    parser.add_argument("--port").required().scan<'i', uint32_t>().required();
});
COMMAND(register_deva) {
    SPAN(span);
    auto host = args.get<std::string>("--host");
    auto ip = args.get<std::string>("--ip");
    auto port = args.get<uint32_t>("--port");

    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.connect_timeout_ms = 2000; // NOLINT(readability-magic-numbers)
    options.timeout_ms = 10000;        // NOLINT(readability-magic-numbers)
    options.max_retry = 0;             // NOLINT(readability-magic-numbers)
    if (channel.Init(host.c_str(), &options) != 0) {
        return Status(EAGAIN, "Fail to initialize channel");
    }

    brpc::Controller cntl;
    pain::proto::asura::RegisterDevaRequest request;
    pain::proto::asura::RegisterDevaResponse response;
    pain::proto::asura::AsuraService::Stub stub(&channel);
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

REGISTER_ASURA_CMD(list_deva, [](argparse::ArgumentParser& parser) {
    parser.add_description("list deva");
});
COMMAND(list_deva) {
    SPAN(span);
    auto host = args.get<std::string>("--host");

    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.connect_timeout_ms = 2000; // NOLINT(readability-magic-numbers)
    options.timeout_ms = 10000;        // NOLINT(readability-magic-numbers)
    options.max_retry = 0;             // NOLINT(readability-magic-numbers)
    if (channel.Init(host.c_str(), &options) != 0) {
        return Status(EAGAIN, "Fail to initialize channel");
    }

    brpc::Controller cntl;
    pain::proto::asura::ListDevaRequest request;
    pain::proto::asura::ListDevaResponse response;
    pain::proto::asura::AsuraService::Stub stub(&channel);
    pain::inject_tracer(&cntl);

    stub.ListDeva(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        return Status(cntl.ErrorCode(), cntl.ErrorText());
    }

    print(cntl, &response);
    return Status::OK();
}

} // namespace pain::sad::asura
