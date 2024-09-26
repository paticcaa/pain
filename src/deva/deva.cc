#include <braft/raft.h>          // braft::Node braft::StateMachine
#include <braft/storage.h>       // braft::SnapshotWriter
#include <braft/util.h>          // braft::AsyncClosureGuard
#include <brpc/controller.h>     // brpc::Controller
#include <brpc/server.h>         // brpc::Server
#include <butil/sys_byteorder.h> // butil::NetToHost32
#include <fcntl.h>               // open
#include <gflags/gflags.h>       // DEFINE_*
#include <sys/types.h>           // O_CREAT
#include "base/plog.h"

DEFINE_bool(check_term, true, "Check if the leader changed to another term");
DEFINE_bool(disable_cli, false, "Don't allow raft_cli access this node");
DEFINE_bool(log_applied_task, false, "Print notice log when a task is applied");
DEFINE_int32(election_timeout_ms, 5000, "Start election in such milliseconds if disconnect with the leader");
DEFINE_int32(snapshot_interval, 30, "Interval between each snapshot");
DEFINE_string(conf, "", "Initial configuration of the replication group");
DEFINE_string(data_path, "./data", "Path of data stored on");
DEFINE_string(group, "deva", "Id of the replication group");
DECLARE_string(deva_listen_address);

namespace pain::deva {
using Status = butil::Status;

class DevaClosure : public braft::Closure {
public:
    DevaClosure(google::protobuf::Closure* done) : _done(done) {}
    ~DevaClosure() {}

    void set_status(const Status& status) {
        _status = status;
    }
    void Run() {
        if (_done) {
            _done->Run();
        }
        delete this;
    }

private:
    google::protobuf::Closure* _done;
    Status _status;
};

class Deva : public braft::StateMachine {
public:
    Deva() : _node(NULL), _leader_term(-1) {}
    ~Deva() {
        delete _node;
    }

    int start() {
        std::string data_path = FLAGS_data_path + "/data";
        butil::EndPoint addr;
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
        node_options.log_uri = prefix + "/log";
        node_options.raft_meta_uri = prefix + "/raft_meta";
        node_options.snapshot_uri = prefix + "/snapshot";
        node_options.disable_cli = FLAGS_disable_cli;
        braft::Node* node = new braft::Node(FLAGS_group, braft::PeerId(addr));
        if (node->init(node_options) != 0) {
            LOG(ERROR) << "Fail to init raft node";
            delete node;
            return -1;
        }
        _node = node;
        return 0;
    }

    bool is_leader() const {
        return _leader_term.load(butil::memory_order_acquire) > 0;
    }

    void shutdown() {
        if (_node) {
            _node->shutdown(NULL);
        }
    }

    void join() {
        if (_node) {
            _node->join();
        }
    }

    void on_apply(braft::Iterator& iter) {
        for (; iter.valid(); iter.next()) {
            braft::AsyncClosureGuard closure_guard(iter.done());
            butil::IOBuf data;
            off_t offset = 0;
            if (iter.done()) {
                DevaClosure* c = dynamic_cast<DevaClosure*>(iter.done());
            } else {
                uint32_t meta_size = 0;
                butil::IOBuf saved_log = iter.data();
                saved_log.cutn(&meta_size, sizeof(uint32_t));
                meta_size = butil::NetToHost32(meta_size);
                butil::IOBuf meta;
                saved_log.cutn(&meta, meta_size);
                butil::IOBufAsZeroCopyInputStream wrapper(meta);
            }

            LOG_IF(INFO, FLAGS_log_applied_task) << "Write " << data.size() << " bytes"
                                                 << " from offset=" << offset << " at log_index=" << iter.index();
        }
    }

    struct SnapshotArg {
        braft::SnapshotWriter* writer;
        braft::Closure* done;
    };

    static void* save_snapshot(void* arg) {
        SnapshotArg* sa = (SnapshotArg*)arg;
        std::unique_ptr<SnapshotArg> arg_guard(sa);
        brpc::ClosureGuard done_guard(sa->done);
        std::string snapshot_path = sa->writer->get_path() + "/data";
        return NULL;
    }

    void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
        SnapshotArg* arg = new SnapshotArg;
        arg->writer = writer;
        arg->done = done;
        bthread_t tid;
        bthread_start_urgent(&tid, NULL, save_snapshot, arg);
    }

    int on_snapshot_load(braft::SnapshotReader* reader) {
        CHECK(!is_leader()) << "Leader is not supposed to load snapshot";
        return 0;
    }

    void on_leader_start(int64_t term) {
        _leader_term.store(term, butil::memory_order_release);
        LOG(INFO) << "Node becomes leader";
    }
    void on_leader_stop(const butil::Status& status) {
        _leader_term.store(-1, butil::memory_order_release);
        LOG(INFO) << "Node stepped down : " << status;
    }

    void on_shutdown() {
        LOG(INFO) << "This node is down";
    }
    void on_error(const ::braft::Error& e) {
        LOG(ERROR) << "Met raft error " << e;
    }
    void on_configuration_committed(const ::braft::Configuration& conf) {
        LOG(INFO) << "Configuration of this group is " << conf;
    }
    void on_stop_following(const ::braft::LeaderChangeContext& ctx) {
        LOG(INFO) << "Node stops following " << ctx;
    }
    void on_start_following(const ::braft::LeaderChangeContext& ctx) {
        LOG(INFO) << "Node start following " << ctx;
    }

private:
    braft::Node* volatile _node;
    butil::atomic<int64_t> _leader_term;
};

} // namespace pain::deva
