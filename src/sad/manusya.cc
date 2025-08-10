#include <brpc/channel.h>
#include <brpc/controller.h>
#include <butil/guid.h>
#include <butil/status.h>
#include <json2pb/pb_to_json.h>
#include <fstream>
#include <fmt/format.h>
#include <argparse/argparse.hpp>

#include "pain/proto/manusya.pb.h"
#include "base/tracer.h"
#include "base/types.h"
#include "base/uuid.h"
#include "sad/common.h"
#include "sad/macro.h"

#define REGISTER_MANUSYA_CMD(cmd, ...) REGISTER(cmd, manusya_parser, DEFER(__VA_ARGS__))

namespace pain::sad {
argparse::ArgumentParser& program();
}
namespace pain::sad::manusya {
argparse::ArgumentParser manusya_parser("manusya", "1.0", argparse::default_arguments::none);

EXECUTE(program().add_subparser(manusya_parser));
EXECUTE(manusya_parser.add_description("send cmd to manusya server")
            .add_argument("--host")
            .default_value(std::string("127.0.0.1:8003")));

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

REGISTER_MANUSYA_CMD(create_chunk, [](argparse::ArgumentParser& parser) {
    parser.add_description("create chunk");
    parser.add_argument("--enable-append-out-of-order").default_value(false).implicit_value(true);
});
COMMAND(create_chunk) {
    SPAN(span);
    auto host = args.get<std::string>("--host");
    auto append_out_of_order = args.get<bool>("--enable-append-out-of-order");
    auto digest = args.get<bool>("--enable-digest");
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.connect_timeout_ms = 2000;
    options.timeout_ms = 10000;
    options.max_retry = 0;
    if (channel.Init(host.c_str(), &options) != 0) {
        return Status(EAGAIN, "Fail to initialize channel");
    }

    brpc::Controller cntl;
    pain::proto::manusya::CreateChunkRequest request;
    pain::proto::manusya::CreateChunkResponse response;
    pain::proto::manusya::ManusyaService_Stub stub(&channel);
    pain::inject_tracer(&cntl);

    request.mutable_chunk_options()->set_append_out_of_order(append_out_of_order);
    request.mutable_chunk_options()->set_digest(digest);
    stub.CreateChunk(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        return Status(cntl.ErrorCode(), cntl.ErrorText());
    }

    print(cntl, &response, [](Json& out) {
        uint64_t low = out["uuid"]["low"];
        uint64_t high = out["uuid"]["high"];
        pain::UUID uuid(high, low);
        out["uuid"] = uuid.str();
    });
    return Status::OK();
}

REGISTER_MANUSYA_CMD(append_chunk, [](argparse::ArgumentParser& parser) {
    parser.add_description("append chunk");
    parser.add_argument("-c", "--chunk-id").required().help("chunk uuid, such as 123e4567-e89b-12d3-a456-426655440000");
    parser.add_argument("-o", "--offset").default_value(0ul).help("offset to append data").scan<'i', uint64_t>();
    parser.add_argument("-d", "--data").required().help("data to append");
});
COMMAND(append_chunk) {
    SPAN(span);
    auto chunk_id = args.get<std::string>("--chunk-id");
    auto host = args.get<std::string>("--host");
    auto data = args.get<std::string>("--data");
    auto offset = args.get<uint64_t>("--offset");

    if (!UUID::valid(chunk_id)) {
        return Status(EINVAL, "Invalid chunk id");
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
    pain::proto::manusya::AppendChunkRequest request;
    pain::proto::manusya::AppendChunkResponse response;
    pain::proto::manusya::ManusyaService_Stub stub(&channel);
    inject_tracer(&cntl);

    auto uuid = pain::UUID::from_str_or_die(chunk_id);
    request.set_offset(offset);
    request.mutable_uuid()->set_low(uuid.low());
    request.mutable_uuid()->set_high(uuid.high());
    cntl.request_attachment().append(data);
    stub.AppendChunk(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        return Status(cntl.ErrorCode(), cntl.ErrorText());
    }

    print(cntl, &response);
    return Status::OK();
}

REGISTER_MANUSYA_CMD(list_chunk, [](argparse::ArgumentParser& parser) {
    parser.add_description("list chunk");
    parser.add_argument("--start").default_value(std::string("00000000-0000-0000-0000-000000000000"));
    parser.add_argument("--limit").default_value(10u).scan<'i', uint32_t>();
});
COMMAND(list_chunk) {
    SPAN(span);
    auto host = args.get<std::string>("--host");
    auto start = args.get<std::string>("--start");
    auto limit = args.get<uint32_t>("--limit");

    if (!UUID::valid(start)) {
        return Status(EINVAL, "Invalid start id");
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
    pain::proto::manusya::ListChunkRequest request;
    pain::proto::manusya::ListChunkResponse response;
    pain::proto::manusya::ManusyaService::Stub stub(&channel);
    inject_tracer(&cntl);

    auto uuid = pain::UUID::from_str_or_die(start);
    request.mutable_start()->set_low(uuid.low());
    request.mutable_start()->set_high(uuid.high());
    request.set_limit(limit);

    stub.ListChunk(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        return Status(cntl.ErrorCode(), cntl.ErrorText());
    }

    print(cntl, &response, [](Json& out) {
        for (auto& uuid : out["uuids"]) {
            uint64_t low = uuid["low"];
            uint64_t high = uuid["high"];
            UUIDv4::UUID uuid_(high, low);
            uuid = uuid_.str();
        }
    });

    return Status::OK();
}

REGISTER_MANUSYA_CMD(read_chunk, [](argparse::ArgumentParser& parser) {
    parser.add_description("read chunk")
        .add_argument("-c", "--chunk-id")
        .required()
        .help("chunk uuid, such as 123e4567-e89b-12d3-a456-426655440000");
    parser.add_argument("--offset").default_value(0ul).help("offset to read data").scan<'i', uint64_t>();
    parser.add_argument("-l", "--length").default_value(1024u).help("length to read data").scan<'i', uint32_t>();
    parser.add_argument("-o", "--output").default_value(std::string("-"));
});
COMMAND(read_chunk) {
    SPAN(span);
    auto chunk_id = args.get<std::string>("--chunk-id");
    auto host = args.get<std::string>("--host");
    auto offset = args.get<uint64_t>("--offset");
    auto length = args.get<uint32_t>("--length");
    auto output = args.get<std::string>("--output");

    if (!UUID::valid(chunk_id)) {
        return Status(EINVAL, "Invalid chunk id");
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
    pain::proto::manusya::ReadChunkRequest request;
    pain::proto::manusya::ReadChunkResponse response;
    pain::proto::manusya::ManusyaService::Stub stub(&channel);
    inject_tracer(&cntl);

    auto uuid = pain::UUID::from_str_or_die(chunk_id);

    request.set_offset(offset);
    request.set_length(length);
    request.mutable_uuid()->set_low(uuid.low());
    request.mutable_uuid()->set_high(uuid.high());
    stub.ReadChunk(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        return Status(cntl.ErrorCode(), cntl.ErrorText());
    }

    print(cntl, &response);

    if (output == "-") {
        fmt::print("{}\n", cntl.response_attachment().to_string());
    } else {
        std::ofstream ofs(output, std::ios::binary);
        ofs.write(cntl.response_attachment().to_string().data(), cntl.response_attachment().size());
    }
    return Status::OK();
}

REGISTER_MANUSYA_CMD(seal_chunk, [](argparse::ArgumentParser& parser) {
    parser.add_description("seal chunk")
        .add_argument("-c", "--chunk-id")
        .required()
        .help("chunk uuid, such as 123e4567-e89b-12d3-a456-426655440000");
});
COMMAND(seal_chunk) {
    SPAN(span);
    auto chunk_id = args.get<std::string>("--chunk-id");
    auto host = args.get<std::string>("--host");

    if (!UUID::valid(chunk_id)) {
        return Status(EINVAL, "Invalid chunk id");
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
    pain::proto::manusya::SealChunkRequest request;
    pain::proto::manusya::SealChunkResponse response;
    pain::proto::manusya::ManusyaService::Stub stub(&channel);
    inject_tracer(&cntl);

    auto uuid = pain::UUID::from_str_or_die(chunk_id);
    request.mutable_uuid()->set_low(uuid.low());
    request.mutable_uuid()->set_high(uuid.high());
    stub.SealChunk(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        return Status(cntl.ErrorCode(), cntl.ErrorText());
    }

    print(cntl, &response);
    return Status::OK();
}

REGISTER_MANUSYA_CMD(remove_chunk, [](argparse::ArgumentParser& parser) {
    parser.add_description("remove chunk")
        .add_argument("-c", "--chunk-id")
        .required()
        .help("chunk uuid, such as 123e4567-e89b-12d3-a456-426655440000");
});
COMMAND(remove_chunk) {
    SPAN(span);
    auto chunk_id = args.get<std::string>("--chunk-id");
    auto host = args.get<std::string>("--host");

    if (!UUID::valid(chunk_id)) {
        return Status(EINVAL, "Invalid chunk id");
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
    pain::proto::manusya::RemoveChunkRequest request;
    pain::proto::manusya::RemoveChunkResponse response;
    pain::proto::manusya::ManusyaService::Stub stub(&channel);
    inject_tracer(&cntl);

    auto uuid = pain::UUID::from_str_or_die(chunk_id);
    request.mutable_uuid()->set_low(uuid.low());
    request.mutable_uuid()->set_high(uuid.high());
    stub.RemoveChunk(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        return Status(cntl.ErrorCode(), cntl.ErrorText());
    }

    print(cntl, &response);
    return Status::OK();
}

REGISTER_MANUSYA_CMD(query_chunk, [](argparse::ArgumentParser& parser) {
    parser.add_description("query chunk")
        .add_argument("-c", "--chunk-id")
        .required()
        .help("chunk uuid, such as 123e4567-e89b-12d3-a456-426655440000");
});
COMMAND(query_chunk) {
    SPAN(span);
    auto chunk_id = args.get<std::string>("--chunk-id");
    auto host = args.get<std::string>("--host");

    if (!UUID::valid(chunk_id)) {
        return Status(EINVAL, "Invalid chunk id");
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
    pain::proto::manusya::QueryChunkRequest request;
    pain::proto::manusya::QueryChunkResponse response;
    pain::proto::manusya::ManusyaService::Stub stub(&channel);
    inject_tracer(&cntl);

    auto uuid = pain::UUID::from_str_or_die(chunk_id);
    request.mutable_uuid()->set_low(uuid.low());
    request.mutable_uuid()->set_high(uuid.high());
    stub.QueryChunk(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        return Status(cntl.ErrorCode(), cntl.ErrorText());
    }

    print(cntl, &response);
    return Status::OK();
}

} // namespace pain::sad::manusya
