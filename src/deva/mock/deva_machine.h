#include <brpc/server.h>
#include <butil/debug/leak_annotations.h>
#include <pain/base/types.h>
#include "common/rocksdb_store.h"
#include "common/rsm/rsm.h"
#include "deva/deva.h"
#include "deva/deva_service_impl.h"

namespace pain::deva::mock {

class DevaMachine {
public:
    DevaMachine(const char* data_path, const char* group, const char* address, const char* node_conf) :
        _address(address),
        _node_conf(node_conf) {
        braft::NodeOptions node_options;
        if (node_options.initial_conf.parse_from(node_conf) != 0) {
            BOOST_ASSERT_MSG(false, "Fail to parse configuration");
        }
        butil::EndPoint addr;
        if (butil::str2endpoint(_address.c_str(), &addr) != 0) {
            BOOST_ASSERT_MSG(false, "Fail to parse address");
        }

        node_options.election_timeout_ms = 1000; // NOLINT
        node_options.node_owns_fsm = false;
        node_options.snapshot_interval_s = 30; // NOLINT
        std::string prefix = fmt::format("local://{}", data_path);
        node_options.log_uri = fmt::format("{}/{}/log", prefix, group);
        node_options.raft_meta_uri = fmt::format("{}/{}/raft_meta", prefix, group);
        node_options.snapshot_uri = fmt::format("{}/{}/snapshot", prefix, group);

        std::string rocksdb_path = fmt::format("{}/{}/db", data_path, group);
        // remove rocksdb path
        std::filesystem::remove_all(rocksdb_path);
        common::RocksdbStorePtr store;
        auto status = common::RocksdbStore::open(rocksdb_path.c_str(), &store);
        BOOST_ASSERT_MSG(status.ok(), "Fail to open rocksdb store");
        _rsm = new common::Rsm(addr, group, node_options, new Deva(store));
    }

    Status start() {
        auto deva_service_impl = new pain::deva::DevaServiceImpl(_rsm);
        if (_server.AddService(deva_service_impl, brpc::SERVER_OWNS_SERVICE) != 0) {
            return Status(EINVAL, "Fail to add service");
        }

        if (braft::add_service(&_server, _address.c_str()) != 0) {
            return Status(EINVAL, "Fail to add raft service");
        }

        brpc::ServerOptions options;
        options.idle_timeout_sec = -1;
        ANNOTATE_SCOPED_MEMORY_LEAK;
        if (_server.Start(_address.c_str(), &options) != 0) {
            return Status(EINVAL, "Fail to start EchoServer");
        }

        _rsm->start();
        return Status::OK();
    }

    void stop() {
        _server.Stop(0);
        _server.Join();
        _rsm->shutdown();
        _rsm->join();
    }

private:
    std::string _address;
    std::string _node_conf;
    brpc::Server _server;
    common::RsmPtr _rsm;
};

} // namespace pain::deva::mock
