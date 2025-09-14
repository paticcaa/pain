#pragma once

#include <braft/raft.h>    // braft::Node braft::StateMachine
#include <braft/storage.h> // braft::SnapshotWriter
#include <boost/intrusive_ptr.hpp>
#include "common/rsm/container.h"

namespace pain::common {

class Rsm;
using RsmPtr = boost::intrusive_ptr<Rsm>;
class Rsm : public braft::StateMachine {
public:
    Rsm(const butil::EndPoint& address,
        const std::string& group,
        const braft::NodeOptions& node_options,
        ContainerPtr container);
    ~Rsm();

    int start();

    bool is_leader() const;

    void shutdown();

    void join();

    void apply(const braft::Task& task);
    void on_apply(braft::Iterator& iter) override;

    struct SnapshotArg {
        braft::SnapshotWriter* writer;
        braft::Closure* done;
        ContainerPtr container;
    };

    static void* save_snapshot(void* arg);

    void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) override;

    int on_snapshot_load(braft::SnapshotReader* reader) override;

    void on_leader_start(int64_t term) override;
    void on_leader_stop(const butil::Status& status) override;

    void on_shutdown() override;
    void on_error(const ::braft::Error& e) override;
    void on_configuration_committed(const ::braft::Configuration& conf) override;
    void on_stop_following(const ::braft::LeaderChangeContext& ctx) override;
    void on_start_following(const ::braft::LeaderChangeContext& ctx) override;

    ContainerPtr container() {
        return _container;
    }

private:
    butil::EndPoint _address;
    std::string _group;
    braft::NodeOptions _node_options;
    braft::Node* volatile _node;
    butil::atomic<int64_t> _leader_term;
    std::atomic<int> _use_count = {0};

    friend void intrusive_ptr_add_ref(Rsm* rsm) {
        ++rsm->_use_count;
    }

    friend void intrusive_ptr_release(Rsm* rsm) {
        if (rsm->_use_count.fetch_sub(1) == 1) {
            delete rsm;
        }
    }

    ContainerPtr _container;
};

} // namespace pain::common
