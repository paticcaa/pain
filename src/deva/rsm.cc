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
#include "deva/container.h"
#include "deva/container_op.h"
#include "deva/deva.h"

DEFINE_bool(check_term, true, "Check if the leader changed to another term");
DEFINE_bool(disable_cli, false, "Don't allow raft_cli access this node");
DEFINE_bool(log_applied_task, false, "Print notice log when a task is applied");
DEFINE_int32(election_timeout_ms, 5000, "Start election in such milliseconds if disconnect with the leader");
DEFINE_int32(snapshot_interval, 30, "Interval between each snapshot");
DEFINE_string(conf, "", "Initial configuration of the replication group");
DEFINE_string(data_path, "./data", "Path of data stored on");
DECLARE_string(deva_listen_address);

namespace pain::deva {

Rsm::Rsm(const std::string& group, ContainerPtr container) :
    _node(nullptr),
    _leader_term(-1),
    _group(group),
    _container(container) {}
Rsm::~Rsm() {
    delete _node;
}

int Rsm::start() {
    std::string data_path = FLAGS_data_path + "/data";
    butil::EndPoint addr;
    // NOLINTNEXTLINE
    int r = butil::str2endpoint(FLAGS_deva_listen_address.c_str(), &addr);
    if (r != 0) {
        PLOG_ERROR(("desc", "invalid xbs-meta address")("address", FLAGS_deva_listen_address));
        return -1;
    }
    braft::NodeOptions node_options;
    if (node_options.initial_conf.parse_from(FLAGS_conf) != 0) {
        LOG(ERROR) << "Fail to parse configuration `" << FLAGS_conf << '\'';
        return -1;
    }
    node_options.election_timeout_ms = FLAGS_election_timeout_ms;
    node_options.fsm = this;
    node_options.node_owns_fsm = false;
    node_options.snapshot_interval_s = FLAGS_snapshot_interval;
    std::string prefix = "local://" + FLAGS_data_path;
    node_options.log_uri = fmt::format("{}/{}/log", prefix, _group);
    node_options.raft_meta_uri = fmt::format("{}/{}/raft_meta", prefix, _group);
    node_options.snapshot_uri = fmt::format("{}/{}/snapshot", prefix, _group);
    node_options.disable_cli = FLAGS_disable_cli;
    braft::Node* node = new braft::Node(_group, braft::PeerId(addr));
    if (node->init(node_options) != 0) {
        LOG(ERROR) << "Fail to init raft node";
        delete node;
        return -1;
    }
    _node = node;
    return 0;
}

bool Rsm::is_leader() const {
    if (_node == nullptr) {
        return false;
    }
    return _node->is_leader();
}

void Rsm::shutdown() {
    if (_node != nullptr) {
        _node->shutdown(nullptr);
    }
}

void Rsm::join() {
    if (_node != nullptr) {
        _node->join();
    }
}

void Rsm::apply(const braft::Task& task) {
    if (_node != nullptr) {
        _node->apply(task);
    }
}

void Rsm::on_apply(braft::Iterator& iter) {
    for (; iter.valid(); iter.next()) {
        braft::AsyncClosureGuard closure_guard(iter.done());
        butil::IOBuf data;
        off_t offset = 0;
        if (iter.done() != nullptr) {
            // Run at closure_guard destructed
            auto c = static_cast<OpClosure*>(iter.done());
            c->set_index(iter.index());
        } else {
            butil::IOBuf saved_log = iter.data();
            // clang-format off
            auto op = decode(&saved_log, [rsm = RsmPtr(this)](OpType op_type, IOBuf* buf) {
                return decode(op_type, buf, rsm);
            });
            // clang-format on
            op->on_apply(iter.index());
        }

        LOG(INFO) << "Write " << data.size() << " bytes"
                  << " from offset=" << offset << " at log_index=" << iter.index();
    }
}

struct SnapshotArg {
    braft::SnapshotWriter* writer;
    braft::Closure* done;
};

void* Rsm::save_snapshot(void* arg) {
    SnapshotArg* sa = (SnapshotArg*)arg;
    std::unique_ptr<SnapshotArg> arg_guard(sa);
    brpc::ClosureGuard done_guard(sa->done);
    std::string snapshot_path = sa->writer->get_path() + "/data";
    return nullptr;
}

void Rsm::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
    SnapshotArg* arg = new SnapshotArg;
    arg->writer = writer;
    arg->done = done;
    bthread_t tid = 0;
    bthread_start_urgent(&tid, nullptr, save_snapshot, arg);
}

// NOLINTNEXTLINE
int Rsm::on_snapshot_load(braft::SnapshotReader* reader) {
    std::ignore = reader;
    CHECK(!is_leader()) << "Leader is not supposed to load snapshot";
    return 0;
}

void Rsm::on_leader_start(int64_t term) {
    _leader_term.store(term, butil::memory_order_release);
    LOG(INFO) << "Node becomes leader";
}
void Rsm::on_leader_stop(const butil::Status& status) {
    _leader_term.store(-1, butil::memory_order_release);
    LOG(INFO) << "Node stepped down : " << status;
}

void Rsm::on_shutdown() {
    LOG(INFO) << "This node is down";
}
void Rsm::on_error(const ::braft::Error& e) {
    LOG(ERROR) << "Met raft error " << e;
}
void Rsm::on_configuration_committed(const ::braft::Configuration& conf) {
    LOG(INFO) << "Configuration of this group is " << conf;
}
void Rsm::on_stop_following(const ::braft::LeaderChangeContext& ctx) {
    LOG(INFO) << "Node stops following " << ctx;
}
void Rsm::on_start_following(const ::braft::LeaderChangeContext& ctx) {
    LOG(INFO) << "Node start following " << ctx;
}

RsmPtr default_rsm() {
    static RsmPtr s_rsm = new Rsm("default", new Deva());
    return s_rsm;
}

} // namespace pain::deva
