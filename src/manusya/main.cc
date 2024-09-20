#include <brpc/server.h>
#include "base/scope_exit.h"
#include "base/tracer.h"
#include "manusya/manusya_service_impl.h"

DEFINE_int32(port, 8003, "TCP Port of this server");
DEFINE_int32(idle_timeout_s, -1,
             "Connection will be closed if there is no "
             "read/write operations during the last `idle_timeout_s'");

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    brpc::Server server;

    pain::manusya::ManusyaServiceImpl manusya_service_impl;
    pain::init_tracer("manusya");
    auto stop_tracer =
        pain::make_scope_exit([]() { pain::cleanup_tracer(); });

    if (server.AddService(&manusya_service_impl,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }

    brpc::ServerOptions options;
    options.idle_timeout_sec = FLAGS_idle_timeout_s;
    if (server.Start(FLAGS_port, &options) != 0) {
        LOG(ERROR) << "Fail to start EchoServer";
        return -1;
    }

    server.RunUntilAskedToQuit();
    return 0;
}
