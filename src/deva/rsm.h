#pragma once

#include <braft/raft.h>    // braft::Node braft::StateMachine
#include <braft/storage.h> // braft::SnapshotWriter
#include <boost/intrusive_ptr.hpp>
#include "base/types.h"

namespace pain::deva {

class Rsm;
using RsmPtr = boost::intrusive_ptr<Rsm>;
class Rsm : public braft::StateMachine {
public:
    Rsm(int id);
    ~Rsm();

    int id() const {
        return _id;
    }
    int start();

    bool is_leader() const;

    void shutdown();

    void join();

    void apply(const braft::Task& task);
    void on_apply(braft::Iterator& iter);

    struct SnapshotArg {
        braft::SnapshotWriter* writer;
        braft::Closure* done;
    };

    static void* save_snapshot(void* arg);

    void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done);

    int on_snapshot_load(braft::SnapshotReader* reader);

    void on_leader_start(int64_t term);
    void on_leader_stop(const butil::Status& status);

    void on_shutdown();
    void on_error(const ::braft::Error& e);
    void on_configuration_committed(const ::braft::Configuration& conf);
    void on_stop_following(const ::braft::LeaderChangeContext& ctx);
    void on_start_following(const ::braft::LeaderChangeContext& ctx);

private:
    braft::Node* volatile _node;
    butil::atomic<int64_t> _leader_term;
    std::atomic<int> _use_count = {0};
    int _id = 0;

    friend void intrusive_ptr_add_ref(Rsm* rsm) {
        ++rsm->_use_count;
    }

    friend void intrusive_ptr_release(Rsm* rsm) {
        if (rsm->_use_count.fetch_sub(1) == 1) {
            delete rsm;
        }
    }
};

RsmPtr default_rsm();

} // namespace pain::deva
