#include <braft/raft.h>
#include <brpc/server.h>
#include <pain/base/plog.h>
#include <pain/base/scope_exit.h>
#include <pain/base/spdlog_sink.h>
#include <pain/base/tracer.h>
#include "deva/deva_service_impl.h"
#include "deva/rsm.h"

DECLARE_string(rsm_listen_address);
DEFINE_int32(idle_timeout_s,
             -1,
             "Connection will be closed if there is no "
             "read/write operations during the last `idle_timeout_s'");
DEFINE_string(log_level, "debug", "Log level");

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    pain::LoggerOptions logger_options = {
        .file_name = "deva.log",
        .name = "deva",
        .level_log = spdlog::level::from_str(FLAGS_log_level),
        .async_threads = 1,
    };
    static auto s_flush_log = pain::make_logger(logger_options);

    static pain::SpdlogSink s_spdlog_sink;
    logging::SetLogSink(&s_spdlog_sink);

    brpc::Server server;

    pain::deva::DevaServiceImpl deva_service_impl;
    pain::init_tracer("deva");
    auto stop_tracer = pain::make_scope_exit([]() {
        pain::cleanup_tracer();
    });

    if (server.AddService(&deva_service_impl, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }

    if (braft::add_service(&server, FLAGS_rsm_listen_address.c_str()) != 0) {
        LOG(ERROR) << "Fail to add raft service";
        return -1;
    }

    brpc::ServerOptions options;
    options.idle_timeout_sec = FLAGS_idle_timeout_s;
    if (server.Start(FLAGS_rsm_listen_address.c_str(), &options) != 0) {
        LOG(ERROR) << "Fail to start EchoServer";
        return -1;
    }

    auto rsm = pain::deva::default_rsm();
    rsm->start();
    server.RunUntilAskedToQuit();
    rsm->shutdown();
    server.Stop(0);
    rsm->join();
    server.Join();
    return 0;
}
