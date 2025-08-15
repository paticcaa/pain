#include <brpc/channel.h>
#include <butil/logging.h>
#include <butil/time.h>
#include <gflags/gflags.h>
#include <cstdlib>
#include <fmt/format.h>
#include <argparse/argparse.hpp>

#include "base/plog.h"
#include "base/scope_exit.h"
#include "base/spdlog_sink.h"
#include "base/tracer.h"
#include "base/types.h"
#include "sad/common.h"
#include "sad/macro.h"
#include "spdlog/common.h"

namespace pain::sad {
argparse::ArgumentParser& program();
void init(int argc, char* argv[]);
Status execute(argparse::ArgumentParser& parser);
} // namespace pain::sad

int main(int argc, char* argv[]) {
    pain::sad::init(argc, argv);
    pain::LoggerOptions logger_options = {
        .file_name = "sad.log",
        .name = "sad",
        .level_log = spdlog::level::from_str(pain::sad::program().get<std::string>("--log-level")),
        .async_threads = 1,
    };
    static auto s_flush_log = pain::make_logger(logger_options);

    static pain::SpdlogSink s_spdlog_sink;
    logging::SetLogSink(&s_spdlog_sink);
    PLOG_WARN(("desc", "sad start"));
    pain::init_tracer("sad");
    auto cleanup_tracer = pain::make_scope_exit([] {
        PLOG_WARN(("desc", "sad exit"));
        pain::cleanup_tracer();
    });

    {
        SPAN(span, "sad");
        auto status = pain::sad::execute(pain::sad::program());
        if (!status.ok()) {
            PLOG_ERROR(("desc", "sad exit")("status", status.error_str()));
            pain::sad::print(status);
            return EXIT_FAILURE;
        }
    }
    return 0;
}
