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

#define REGISTER_DEVA_CMD(cmd, ...) REGISTER(cmd, deva_parser(), DEFER(__VA_ARGS__))

namespace pain::sad {
argparse::ArgumentParser& program();
}
namespace pain::sad::deva {
// NOLINTNEXTLINE
argparse::ArgumentParser& deva_parser() {
    static argparse::ArgumentParser s_deva_parser("deva", "1.0", argparse::default_arguments::none);
    return s_deva_parser;
}

EXECUTE(program().add_subparser(deva_parser()));
EXECUTE(deva_parser()
            .add_description("send cmd to deva server")
            .add_argument("--host")
            .default_value(std::string("127.0.0.1:8001")));

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

REGISTER_DEVA_CMD(open, [](argparse::ArgumentParser& parser) {
    parser.add_argument("--path").required();
    parser.add_argument("-c", "--create").default_value(false).implicit_value(true);
});
COMMAND(open) {
    SPAN(span);
    auto host = args.get<std::string>("--host");
    auto path = args.get<std::string>("--path");
    auto create = args.get<bool>("--create");
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.connect_timeout_ms = 2000; // NOLINT(readability-magic-numbers)
    options.timeout_ms = 10000;        // NOLINT(readability-magic-numbers)
    options.max_retry = 0;
    if (channel.Init(host.c_str(), &options) != 0) {
        return Status(EAGAIN, "Fail to initialize channel");
    }

    brpc::Controller cntl;
    pain::proto::deva::OpenFileRequest request;
    pain::proto::deva::OpenFileResponse response;
    pain::proto::deva::DevaService::Stub stub(&channel);
    pain::inject_tracer(&cntl);

    using pain::proto::deva::OpenFlag;
    request.set_path(path);
    if (create) {
        request.set_flags(OpenFlag::OPEN_CREATE | OpenFlag::OPEN_APPEND);
    }

    stub.OpenFile(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        return Status(cntl.ErrorCode(), cntl.ErrorText());
    }

    print(cntl, &response);
    return Status::OK();
}

} // namespace pain::sad::deva
