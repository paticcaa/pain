#include <brpc/channel.h>
#include <brpc/controller.h>
#include <butil/guid.h>
#include <butil/status.h>
#include <core/manusya.pb.h>
#include <json2pb/pb_to_json.h>
#include <fstream>
#include <fmt/format.h>
#include <argparse/argparse.hpp>

#include "base/tracer.h"
#include "base/uuid.h"
#include "sad/common.h"
#include "sad/macro.h"

using Status = butil::Status;

#define MANUSYA_CMD(cmd) REGISTER(cmd, manusya_parser)

namespace pain::sad {
extern argparse::ArgumentParser program;
}
namespace pain::sad::manusya {
argparse::ArgumentParser manusya_parser("manusya", "1.0", argparse::default_arguments::none);

RUN(program.add_subparser(manusya_parser));
RUN(manusya_parser.add_description("send cmd to manusya server")
        .add_argument("--host")
        .default_value(std::string("127.0.0.1:8003")));

static std::map<std::string, std::function<Status(argparse::ArgumentParser &)>>
    subcommands = {};

void add(const std::string &name,
         std::function<Status(argparse::ArgumentParser &parser)> func) {
    std::string name_;
    for (auto c : name) {
        if (c == '_') {
            c = '-';
        }
        name_ += c;
    }
    subcommands[name_] = func;
}

Status execute(argparse::ArgumentParser &parser) {
    for (const auto &[name, func] : subcommands) {
        if (parser.is_subcommand_used(name)) {
            SPAN(span, name);
            return func(parser.at<argparse::ArgumentParser>(name));
        }
    }
    std::cerr << parser;
    std::exit(1);
}

MANUSYA_CMD(create_chunk)
RUN(ARGS(create_chunk).add_description("create chunk"))
COMMAND(create_chunk) {
    SPAN(span);
    auto host = args.get<std::string>("--host");
    brpc::Channel channel;
    brpc::ChannelOptions options;
    if (channel.Init(host.c_str(), &options) != 0) {
        return Status(EAGAIN, "Fail to initialize channel");
    }

    brpc::Controller cntl;
    pain::manusya::CreateChunkRequest request;
    pain::manusya::CreateChunkResponse response;
    pain::manusya::ManusyaService_Stub stub(&channel);
    base::inject_tracer(&cntl);
    stub.create_chunk(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        return Status(cntl.ErrorCode(), cntl.ErrorText());
    }

    print(cntl, &response, [](json &out) {
        uint64_t low = out["uuid"]["low"];
        uint64_t high = out["uuid"]["high"];
        pain::base::UUID uuid(high, low);
        out["uuid"] = uuid.str();
    });
    return Status::OK();
}

MANUSYA_CMD(append_chunk)
RUN(ARGS(append_chunk)
        .add_description("append chunk")
        .add_argument("-c", "--chunk-id")
        .required()
        .help("chunk uuid, such as 123e4567-e89b-12d3-a456-426655440000"))
RUN(ARGS(append_chunk)
        .add_argument("-o", "--offset")
        .default_value(0ul)
        .help("offset to append data")
        .scan<'i', uint64_t>())
RUN(ARGS(append_chunk)
        .add_argument("-d", "--data")
        .required()
        .help("data to append"))
COMMAND(append_chunk) {
    SPAN(span);
    auto chunk_id = args.get<std::string>("--chunk-id");
    auto host = args.get<std::string>("--host");
    auto data = args.get<std::string>("--data");
    auto offset = args.get<uint64_t>("--offset");

    if (!base::UUID::valid(chunk_id)) {
        return Status(EINVAL, "Invalid chunk id");
    }

    brpc::Channel channel;
    brpc::ChannelOptions options;
    if (channel.Init(host.c_str(), &options) != 0) {
        return Status(EAGAIN, "Fail to initialize channel");
    }

    brpc::Controller cntl;
    pain::manusya::AppendChunkRequest request;
    pain::manusya::AppendChunkResponse response;
    pain::manusya::ManusyaService_Stub stub(&channel);
    base::inject_tracer(&cntl);

    auto uuid = pain::base::UUID::from_str_or_die(chunk_id);
    request.set_offset(offset);
    request.mutable_uuid()->set_low(uuid.low());
    request.mutable_uuid()->set_high(uuid.high());
    cntl.request_attachment().append(data);
    stub.append_chunk(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        return Status(cntl.ErrorCode(), cntl.ErrorText());
    }

    print(cntl, &response);
    return Status::OK();
}

MANUSYA_CMD(list_chunk)
RUN(ARGS(list_chunk).add_description("list chunk"))
COMMAND(list_chunk) {
    SPAN(span);
    auto host = args.get<std::string>("--host");
    brpc::Channel channel;
    brpc::ChannelOptions options;
    if (channel.Init(host.c_str(), &options) != 0) {
        return Status(EAGAIN, "Fail to initialize channel");
    }

    brpc::Controller cntl;
    pain::manusya::ListChunkRequest request;
    pain::manusya::ListChunkResponse response;
    pain::manusya::ManusyaService::Stub stub(&channel);
    base::inject_tracer(&cntl);
    stub.list_chunk(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        return Status(cntl.ErrorCode(), cntl.ErrorText());
    }

    print(cntl, &response, [](json &out) {
        for (auto &uuid : out["uuids"]) {
            uint64_t low = uuid["low"];
            uint64_t high = uuid["high"];
            UUIDv4::UUID uuid_(high, low);
            uuid = uuid_.str();
        }
    });

    return Status::OK();
}

MANUSYA_CMD(read_chunk)
RUN(ARGS(read_chunk)
        .add_description("read chunk")
        .add_argument("-c", "--chunk-id")
        .required()
        .help("chunk uuid, such as 123e4567-e89b-12d3-a456-426655440000"))
RUN(ARGS(read_chunk)
        .add_argument("--offset")
        .default_value(0ul)
        .help("offset to read data")
        .scan<'i', uint64_t>())
RUN(ARGS(read_chunk)
        .add_argument("-l", "--length")
        .default_value(1024u)
        .help("length to read data")
        .scan<'i', uint32_t>())
RUN(ARGS(read_chunk)
        .add_argument("-o", "--output")
        .default_value(std::string("-")))
COMMAND(read_chunk) {
    SPAN(span);
    auto chunk_id = args.get<std::string>("--chunk-id");
    auto host = args.get<std::string>("--host");
    auto offset = args.get<uint64_t>("--offset");
    auto length = args.get<uint32_t>("--length");
    auto output = args.get<std::string>("--output");

    if (!base::UUID::valid(chunk_id)) {
        return Status(EINVAL, "Invalid chunk id");
    }

    brpc::Channel channel;
    brpc::ChannelOptions options;
    if (channel.Init(host.c_str(), &options) != 0) {
        return Status(EAGAIN, "Fail to initialize channel");
    }

    brpc::Controller cntl;
    pain::manusya::ReadChunkRequest request;
    pain::manusya::ReadChunkResponse response;
    pain::manusya::ManusyaService::Stub stub(&channel);
    base::inject_tracer(&cntl);

    auto uuid = pain::base::UUID::from_str_or_die(chunk_id);

    request.set_offset(offset);
    request.set_length(length);
    request.mutable_uuid()->set_low(uuid.low());
    request.mutable_uuid()->set_high(uuid.high());
    stub.read_chunk(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        return Status(cntl.ErrorCode(), cntl.ErrorText());
    }

    print(cntl, &response);

    if (output == "-") {
        fmt::print("{}\n", cntl.response_attachment().to_string());
    } else {
        std::ofstream ofs(output, std::ios::binary);
        ofs.write(cntl.response_attachment().to_string().data(),
                  cntl.response_attachment().size());
    }
    return Status::OK();
}

} // namespace pain::sad::manusya