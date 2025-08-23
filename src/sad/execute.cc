#include <fmt/format.h>
#include <argparse/argparse.hpp>

#include <pain/base/tracer.h>
#include <pain/base/types.h>
#include "sad/macro.h"

namespace pain::sad {
namespace manusya {
Status execute(argparse::ArgumentParser& parser);
} // namespace manusya

namespace deva {
Status execute(argparse::ArgumentParser& parser);
} // namespace deva

namespace asura {
Status execute(argparse::ArgumentParser& parser);
} // namespace asura

Status execute(argparse::ArgumentParser& parser) {
    if (parser.is_subcommand_used("manusya")) {
        SPAN(span, "manusya");
        return manusya::execute(parser.at<argparse::ArgumentParser>("manusya"));
    }

    if (parser.is_subcommand_used("deva")) {
        SPAN(span, "deva");
        return deva::execute(parser.at<argparse::ArgumentParser>("deva"));
    }

    if (parser.is_subcommand_used("asura")) {
        SPAN(span, "asura");
        return asura::execute(parser.at<argparse::ArgumentParser>("asura"));
    }
    std::cerr << parser;
    std::exit(1);
}

} // namespace pain::sad
