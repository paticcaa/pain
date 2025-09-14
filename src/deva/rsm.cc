#include "deva/rsm.h"

#include <braft/raft.h>          // braft::Node braft::StateMachine
#include <braft/storage.h>       // braft::SnapshotWriter
#include <braft/util.h>          // braft::AsyncClosureGuard
#include <brpc/controller.h>     // brpc::Controller
#include <brpc/server.h>         // brpc::Server
#include <butil/sys_byteorder.h> // butil::NetToHost32
#include <fcntl.h>               // open
#include <gflags/gflags.h>       // DEFINE_*
#include <pain/base/plog.h>
#include <sys/types.h> // O_CREAT
#include "common/rocksdb_store.h"
#include "deva/deva.h"

DEFINE_bool(rsm_check_term, true, "Check if the leader changed to another term");
DEFINE_bool(rsm_disable_cli, false, "Don't allow raft_cli access this node");
DEFINE_bool(rsm_log_applied_task, false, "Print notice log when a task is applied");
DEFINE_int32(rsm_election_timeout_ms, 5000, "Start election in such milliseconds if disconnect with the leader");
DEFINE_int32(rsm_snapshot_interval, 30, "Interval between each snapshot");
DEFINE_string(rsm_conf, "", "Initial configuration of the replication group");
DEFINE_string(rsm_data_path, "./data", "Path of data stored on");
DEFINE_string(rsm_listen_address, "127.0.0.1:8001", "Listen address of deva");

namespace pain::deva {

common::RsmPtr default_rsm() {
    std::string data_path = FLAGS_rsm_data_path + "/data";
    butil::EndPoint addr;
    std::string group = "default";
    int r = butil::str2endpoint(FLAGS_rsm_listen_address.c_str(), &addr);
    if (r != 0) {
        PLOG_ERROR(("desc", "invalid xbs-meta address")("address", FLAGS_rsm_listen_address));
        BOOST_ASSERT_MSG(false, "invalid xbs-meta address");
    }
    braft::NodeOptions node_options;
    if (node_options.initial_conf.parse_from(FLAGS_rsm_conf) != 0) {
        LOG(ERROR) << "Fail to parse configuration `" << FLAGS_rsm_conf << '\'';
        BOOST_ASSERT_MSG(false, "Fail to parse configuration");
    }
    node_options.election_timeout_ms = FLAGS_rsm_election_timeout_ms;
    node_options.node_owns_fsm = false;
    node_options.snapshot_interval_s = FLAGS_rsm_snapshot_interval;
    std::string prefix = "local://" + FLAGS_rsm_data_path;
    node_options.log_uri = fmt::format("{}/{}/log", prefix, group);
    node_options.raft_meta_uri = fmt::format("{}/{}/raft_meta", prefix, group);
    node_options.snapshot_uri = fmt::format("{}/{}/snapshot", prefix, group);
    node_options.disable_cli = FLAGS_rsm_disable_cli;

    std::string rocksdb_path = fmt::format("{}/{}/db", prefix, group);
    common::RocksdbStorePtr store;
    auto status = common::RocksdbStore::open(rocksdb_path.c_str(), &store);
    BOOST_ASSERT_MSG(status.ok(), "Fail to open rocksdb store");
    static common::RsmPtr s_rsm = new common::Rsm(addr, group, node_options, new Deva(store));
    return s_rsm;
}

} // namespace pain::deva
