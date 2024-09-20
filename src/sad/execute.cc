#include <butil/status.h>
#include <fmt/format.h>
#include <argparse/argparse.hpp>

#include "base/tracer.h"
#include "sad/macro.h"

using Status = butil::Status;

namespace pain::sad {
namespace manusya {

Status execute(argparse::ArgumentParser &parser);

} // namespace manusya

Status execute(argparse::ArgumentParser &parser) {
    if (parser.is_subcommand_used("manusya")) {
        SPAN(span, "manusya");
        return manusya::execute(parser.at<argparse::ArgumentParser>("manusya"));
    }
    std::cerr << parser;
    std::exit(1);
}

} // namespace pain::sad