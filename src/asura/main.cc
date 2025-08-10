#include <brpc/server.h>
#include "asura/topology_service_impl.h"
#include "base/plog.h"
#include "base/scope_exit.h"
#include "base/spdlog_sink.h"
#include "base/tracer.h"
#include "common/rocksdb_store.h"

DEFINE_string(asura_listen_address, "127.0.0.1:8001", "Listen address of asura");
DEFINE_int32(idle_timeout_s,
             -1,
             "Connection will be closed if there is no "
             "read/write operations during the last `idle_timeout_s'");
DEFINE_string(log_level, "debug", "Log level");
DEFINE_string(data_path, "./data", "Path of data stored on");

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    pain::LoggerOptions logger_options = {
        .file_name = "asura.log",
        .name = "asura",
        .level_log = spdlog::level::from_str(FLAGS_log_level),
        .async_threads = 1,
    };
    static auto flush_log = pain::make_logger(logger_options);

    static pain::SpdlogSink spdlog_sink;
    logging::SetLogSink(&spdlog_sink);

    brpc::Server server;

    pain::common::RocksdbStorePtr store;
    auto status = pain::common::RocksdbStore::open(fmt::format("{}/topology", FLAGS_data_path).c_str(), &store);
    if (!status.ok()) {
        LOG(ERROR) << "Fail to open RocksdbStore";
        return -1;
    }

    pain::asura::TopologyServiceImpl topology_service_impl(store);
    pain::init_tracer("asura");
    auto stop_tracer = pain::make_scope_exit([]() {
        pain::cleanup_tracer();
    });

    if (server.AddService(&topology_service_impl, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }

    brpc::ServerOptions options;
    options.idle_timeout_sec = FLAGS_idle_timeout_s;
    if (server.Start(FLAGS_asura_listen_address.c_str(), &options) != 0) {
        LOG(ERROR) << "Fail to start EchoServer";
        return -1;
    }

    server.RunUntilAskedToQuit();
    server.Stop(0);
    server.Join();

    store->close();
    return 0;
}
