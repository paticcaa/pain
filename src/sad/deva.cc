#include <brpc/channel.h>
#include <brpc/controller.h>
#include <butil/guid.h>
#include <butil/status.h>
#include <json2pb/pb_to_json.h>
#include <fstream>
#include <fmt/format.h>
#include <argparse/argparse.hpp>

#include "pain/proto/deva.pb.h"
#include "base/tracer.h"
#include "base/types.h"
#include "base/uuid.h"
#include "sad/common.h"
#include "sad/macro.h"

#define DEVA_CMD(cmd) REGISTER(cmd, deva_parser)

namespace pain::sad {
argparse::ArgumentParser& program();
}
namespace pain::sad::deva {
argparse::ArgumentParser deva_parser("deva", "1.0", argparse::default_arguments::none);

RUN(program().add_subparser(deva_parser));
RUN(deva_parser.add_description("send cmd to deva server")
        .add_argument("--host")
        .default_value(std::string("127.0.0.1:8001")));

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

DEVA_CMD(open)
RUN(ARGS(open).add_description("open file").add_argument("--uuid").required())
COMMAND(open) {
    SPAN(span);
    auto host = args.get<std::string>("--host");
    auto uuid = args.get<std::string>("--uuid");

    if (!UUID::valid(uuid)) {
        return Status(EINVAL, "Invalid uuid id");
    }

    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.connect_timeout_ms = 2000;
    options.timeout_ms = 10000;
    options.max_retry = 0;
    if (channel.Init(host.c_str(), &options) != 0) {
        return Status(EAGAIN, "Fail to initialize channel");
    }

    brpc::Controller cntl;
    pain::proto::deva::OpenRequest request;
    pain::proto::deva::OpenResponse response;
    pain::proto::deva::DevaService::Stub stub(&channel);
    pain::inject_tracer(&cntl);

    auto id = pain::UUID::from_str_or_die(uuid);
    request.mutable_uuid()->set_low(id.low());
    request.mutable_uuid()->set_high(id.high());
    stub.Open(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        return Status(cntl.ErrorCode(), cntl.ErrorText());
    }

    print(cntl, &response);
    return Status::OK();
}

} // namespace pain::sad::deva
