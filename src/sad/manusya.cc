#include <brpc/channel.h>
#include <brpc/controller.h>
#include <butil/guid.h>
#include <butil/status.h>
#include <json2pb/pb_to_json.h>
#include <fstream>
#include <fmt/format.h>
#include <argparse/argparse.hpp>

#include <pain/base/object_id.h>
#include <pain/base/tracer.h>
#include <pain/base/types.h>
#include "pain/proto/manusya.pb.h"
#include "common/object_id_util.h"
#include "sad/common.h"
#include "sad/macro.h"

#define REGISTER_MANUSYA_CMD(cmd, ...) REGISTER(cmd, manusya_parser(), DEFER(__VA_ARGS__))

namespace pain::sad {
argparse::ArgumentParser& program();
}
namespace pain::sad::manusya {
argparse::ArgumentParser& manusya_parser() {
    static argparse::ArgumentParser s_manusya_parser("manusya", "1.0", argparse::default_arguments::none);
    return s_manusya_parser;
}

EXECUTE(program().add_subparser(manusya_parser()));
EXECUTE(manusya_parser()
            .add_description("send cmd to manusya server")
            .add_argument("--host")
            .default_value(std::string("127.0.0.1:8003")));

// NOLINTNEXTLINE(readability-identifier-naming)
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

REGISTER_MANUSYA_CMD(create_chunk, [](argparse::ArgumentParser& parser) {
    parser.add_description("create chunk");
    parser.add_argument("-p", "--partition-id").default_value(1U).scan<'i', uint32_t>();
});
COMMAND(create_chunk) {
    SPAN(span);
    auto host = args.get<std::string>("--host");
    auto partition_id = args.get<uint32_t>("--partition-id");
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.connect_timeout_ms = 2000; // NOLINT(readability-magic-numbers)
    options.timeout_ms = 10000;        // NOLINT(readability-magic-numbers)
    options.max_retry = 0;
    if (channel.Init(host.c_str(), &options) != 0) {
        return Status(EAGAIN, "Fail to initialize channel");
    }

    brpc::Controller cntl;
    pain::proto::manusya::CreateChunkRequest request;
    pain::proto::manusya::CreateChunkResponse response;
    pain::proto::manusya::ManusyaService_Stub stub(&channel);
    pain::inject_tracer(&cntl);

    request.set_partition_id(partition_id);
    stub.CreateChunk(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        return Status(cntl.ErrorCode(), cntl.ErrorText());
    }

    print(cntl, &response, [&](Json& out) {
        pain::ObjectId chunk_id = common::from_proto(response.chunk_id());
        out["chunk_id"] = chunk_id.str();
    });
    return Status::OK();
}

REGISTER_MANUSYA_CMD(append_chunk, [](argparse::ArgumentParser& parser) {
    parser.add_description("append chunk");
    parser.add_argument("-c", "--chunk-id").required().help("chunk uuid, such as 123e4567-e89b-12d3-a456-426655440000");
    // NOLINTNEXTLINE(modernize-use-nullptr)
    parser.add_argument("-o", "--offset").default_value(0UL).help("offset to append data").scan<'i', uint64_t>();
    parser.add_argument("-d", "--data").required().help("data to append");
});
COMMAND(append_chunk) {
    SPAN(span);
    auto chunk_id = args.get<std::string>("--chunk-id");
    auto host = args.get<std::string>("--host");
    auto data = args.get<std::string>("--data");
    auto offset = args.get<uint64_t>("--offset");

    if (!pain::ObjectId::valid(chunk_id)) {
        return Status(EINVAL, fmt::format("Invalid chunk id: {}", chunk_id));
    }

    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.connect_timeout_ms = 2000; // NOLINT(readability-magic-numbers)
    options.timeout_ms = 10000;        // NOLINT(readability-magic-numbers)
    options.max_retry = 0;
    if (channel.Init(host.c_str(), &options) != 0) {
        return Status(EAGAIN, "Fail to initialize channel");
    }

    brpc::Controller cntl;
    pain::proto::manusya::AppendChunkRequest request;
    pain::proto::manusya::AppendChunkResponse response;
    pain::proto::manusya::ManusyaService_Stub stub(&channel);
    inject_tracer(&cntl);

    auto id = pain::ObjectId::from_str_or_die(chunk_id);
    request.set_offset(offset);
    common::to_proto(id, request.mutable_chunk_id());
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
    parser.add_argument("--limit").default_value(10U).scan<'i', uint32_t>();
});
COMMAND(list_chunk) {
    SPAN(span);
    auto host = args.get<std::string>("--host");
    auto start = args.get<std::string>("--start");
    auto limit = args.get<uint32_t>("--limit");

    if (!pain::ObjectId::valid(start)) {
        return Status(EINVAL, fmt::format("Invalid start id: {}", start));
    }

    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.connect_timeout_ms = 2000; // NOLINT(readability-magic-numbers)
    options.timeout_ms = 10000;        // NOLINT(readability-magic-numbers)
    options.max_retry = 0;
    if (channel.Init(host.c_str(), &options) != 0) {
        return Status(EAGAIN, "Fail to initialize channel");
    }

    brpc::Controller cntl;
    pain::proto::manusya::ListChunkRequest request;
    pain::proto::manusya::ListChunkResponse response;
    pain::proto::manusya::ManusyaService::Stub stub(&channel);
    inject_tracer(&cntl);

    auto id = pain::ObjectId::from_str_or_die(start);
    common::to_proto(id, request.mutable_start());
    request.set_limit(limit);

    stub.ListChunk(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        return Status(cntl.ErrorCode(), cntl.ErrorText());
    }

    print(cntl, &response, [&](Json& out) {
        out["chunk_ids"] = Json::array();
        for (auto& chunk_id : response.chunk_ids()) {
            pain::ObjectId id = common::from_proto(chunk_id);
            out["chunk_ids"].push_back(id.str());
        }
    });

    return Status::OK();
}

REGISTER_MANUSYA_CMD(read_chunk, [](argparse::ArgumentParser& parser) {
    parser.add_description("read chunk")
        .add_argument("-c", "--chunk-id")
        .required()
        .help("chunk uuid, such as 123e4567-e89b-12d3-a456-426655440000");
    // NOLINTNEXTLINE(modernize-use-nullptr)
    parser.add_argument("--offset").default_value(0UL).help("offset to read data").scan<'i', uint64_t>();
    parser.add_argument("-l", "--length").default_value(1024U).help("length to read data").scan<'i', uint32_t>();
    parser.add_argument("-o", "--output").default_value(std::string("-"));
});
COMMAND(read_chunk) {
    SPAN(span);
    auto chunk_id = args.get<std::string>("--chunk-id");
    auto host = args.get<std::string>("--host");
    auto offset = args.get<uint64_t>("--offset");
    auto length = args.get<uint32_t>("--length");
    auto output = args.get<std::string>("--output");

    if (!pain::ObjectId::valid(chunk_id)) {
        return Status(EINVAL, fmt::format("Invalid chunk id: {}", chunk_id));
    }

    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.connect_timeout_ms = 2000; // NOLINT(readability-magic-numbers)
    options.timeout_ms = 10000;        // NOLINT(readability-magic-numbers)
    options.max_retry = 0;
    if (channel.Init(host.c_str(), &options) != 0) {
        return Status(EAGAIN, "Fail to initialize channel");
    }

    brpc::Controller cntl;
    pain::proto::manusya::ReadChunkRequest request;
    pain::proto::manusya::ReadChunkResponse response;
    pain::proto::manusya::ManusyaService::Stub stub(&channel);
    inject_tracer(&cntl);

    auto id = pain::ObjectId::from_str_or_die(chunk_id);

    request.set_offset(offset);
    request.set_length(length);
    common::to_proto(id, request.mutable_chunk_id());
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

    if (!pain::ObjectId::valid(chunk_id)) {
        return Status(EINVAL, fmt::format("Invalid chunk id: {}", chunk_id));
    }

    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.connect_timeout_ms = 2000; // NOLINT(readability-magic-numbers)
    options.timeout_ms = 10000;        // NOLINT(readability-magic-numbers)
    options.max_retry = 0;
    if (channel.Init(host.c_str(), &options) != 0) {
        return Status(EAGAIN, "Fail to initialize channel");
    }

    brpc::Controller cntl;
    pain::proto::manusya::QueryAndSealChunkRequest request;
    pain::proto::manusya::QueryAndSealChunkResponse response;
    pain::proto::manusya::ManusyaService::Stub stub(&channel);
    inject_tracer(&cntl);

    auto id = pain::ObjectId::from_str_or_die(chunk_id);
    common::to_proto(id, request.mutable_chunk_id());
    stub.QueryAndSealChunk(&cntl, &request, &response, nullptr);
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

    if (!pain::ObjectId::valid(chunk_id)) {
        return Status(EINVAL, fmt::format("Invalid chunk id: {}", chunk_id));
    }

    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.connect_timeout_ms = 2000; // NOLINT(readability-magic-numbers)
    options.timeout_ms = 10000;        // NOLINT(readability-magic-numbers)
    options.max_retry = 0;
    if (channel.Init(host.c_str(), &options) != 0) {
        return Status(EAGAIN, "Fail to initialize channel");
    }

    brpc::Controller cntl;
    pain::proto::manusya::RemoveChunkRequest request;
    pain::proto::manusya::RemoveChunkResponse response;
    pain::proto::manusya::ManusyaService::Stub stub(&channel);
    inject_tracer(&cntl);

    auto id = pain::ObjectId::from_str_or_die(chunk_id);
    common::to_proto(id, request.mutable_chunk_id());
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

    if (!pain::ObjectId::valid(chunk_id)) {
        return Status(EINVAL, fmt::format("Invalid chunk id: {}", chunk_id));
    }

    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.connect_timeout_ms = 2000; // NOLINT(readability-magic-numbers)
    options.timeout_ms = 10000;        // NOLINT(readability-magic-numbers)
    options.max_retry = 0;
    if (channel.Init(host.c_str(), &options) != 0) {
        return Status(EAGAIN, "Fail to initialize channel");
    }

    brpc::Controller cntl;
    pain::proto::manusya::QueryChunkRequest request;
    pain::proto::manusya::QueryChunkResponse response;
    pain::proto::manusya::ManusyaService::Stub stub(&channel);
    inject_tracer(&cntl);

    auto id = pain::ObjectId::from_str_or_die(chunk_id);
    common::to_proto(id, request.mutable_chunk_id());
    stub.QueryChunk(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        return Status(cntl.ErrorCode(), cntl.ErrorText());
    }

    print(cntl, &response);
    return Status::OK();
}

} // namespace pain::sad::manusya
