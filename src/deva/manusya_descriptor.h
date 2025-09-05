#pragma once

#include <pain/base/uuid.h>

namespace pain::deva {

struct ManusyaDescriptor {
    enum class AdminState {
        kNormal = 0,
        kDecommissioned = 1,
        kMaintenance = 2,
    };

    std::string ip;
    int32_t port;
    UUID uuid;
    std::string cluster_id;
    std::string network_location; // rack info, like "/rack1" or "/dc1/az1/rack1"

    uint64_t total_capacity;  // total capacity (bytes)
    uint64_t used_space;      // used space (bytes)
    uint64_t remaining_space; // remaining space (bytes)

    time_t last_heartbeat_time; // last heartbeat time
    bool is_alive;              // is alive

    std::vector<UUID> chunks;
    AdminState admin_state; // admin state (normal, decommissioned, maintenance, etc.)

    void update_heartbeat() {
        last_heartbeat_time = time(nullptr);
        is_alive = true;
    }

    // 示例方法：标记为不活跃
    void mark_dead() {
        is_alive = false;
    }
};

} // namespace pain::deva
