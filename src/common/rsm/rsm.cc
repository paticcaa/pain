#include "common/rsm/rsm.h"

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
#include "common/rsm/container.h"
#include "common/rsm/container_op.h"
#include "common/rsm/op.h"

namespace pain::common {

Rsm::Rsm(const butil::EndPoint& address,
         const std::string& group,
         const braft::NodeOptions& node_options,
         ContainerPtr container) :
    _address(address),
    _group(group),
    _node_options(node_options),
    _node(nullptr),
    _leader_term(-1),
    _container(container) {
    _node_options.fsm = this;
}
Rsm::~Rsm() {
    PLOG_INFO(("desc", "destructor rsm") //
              ("group", _group)          //
              ("address", butil::endpoint2str(_address).c_str()));
    delete _node;
}

int Rsm::start() {
    PLOG_INFO(("desc", "start rsm") //
              ("group", _group)     //
              ("address", butil::endpoint2str(_address).c_str()));
    braft::Node* node = new braft::Node(_group, braft::PeerId(_address));
    if (node->init(_node_options) != 0) {
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
            auto op = decode(&saved_log, [rsm = RsmPtr(this)](int32_t version, uint32_t op_type, IOBuf* buf) {
                return decode(version, op_type, buf, rsm);
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
    PLOG_INFO(("desc", "save_snapshot"));
    SnapshotArg* sa = (SnapshotArg*)arg;
    std::unique_ptr<SnapshotArg> arg_guard(sa);
    brpc::ClosureGuard done_guard(sa->done);
    std::string snapshot_path = sa->writer->get_path();
    std::vector<std::string> files;
    auto status = sa->container->save_snapshot(snapshot_path, &files);
    if (!status.ok()) {
        sa->done->status() = status;
        LOG(ERROR) << "Fail to save snapshot to " << snapshot_path;
        return nullptr;
    }
    for (const auto& file : files) {
        PLOG_INFO(("desc", "add_file to snapshot")("file", file));
        sa->writer->add_file(file);
    }
    PLOG_INFO(("desc", "save_snapshot done"));
    return nullptr;
}

void Rsm::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
    PLOG_INFO(("desc", "on_snapshot_save"));
    SnapshotArg* arg = new SnapshotArg;
    arg->writer = writer;
    arg->done = done;
    arg->container = _container;
    bthread_t tid = 0;
    bthread_start_urgent(&tid, nullptr, save_snapshot, arg);
}

// NOLINTNEXTLINE
int Rsm::on_snapshot_load(braft::SnapshotReader* reader) {
    PLOG_INFO(("desc", "on_snapshot_load"));
    CHECK(!is_leader()) << "Leader is not supposed to load snapshot";
    auto path = reader->get_path();
    auto status = _container->load_snapshot(path);
    if (!status.ok()) {
        PLOG_ERROR(("desc", "fail to load snapshot from ")("path", path)("error", status.error_str()));
        return -1;
    }
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

} // namespace pain::common
