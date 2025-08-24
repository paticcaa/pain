#pragma once
#include <pain/base/path.h>
#include "deva/mock/deva_machine.h"

namespace pain::deva::mock {

class MockDeva {
public:
    MockDeva();
    ~MockDeva();
    Status start();
    Status start(int index);
    void stop();
    void stop(int index);
    Status wait_for_leader(std::string* addr, int timeout_ms = 10000); // NOLINT(readability-magic-numbers)
    const std::vector<std::string>& node_addrs() const {
        return _node_addrs;
    }
    const std::string& group() const {
        return _group;
    }
    const std::string& node_conf() const {
        return _node_conf;
    }
    const std::string& data_path() const {
        return _data_path;
    }
    const std::string& data_path(int index) const {
        return _data_paths[index];
    }

private:
    std::string _group;
    std::string _node_conf;
    std::string _data_path;
    std::vector<std::string> _data_paths;
    std::vector<std::string> _node_addrs;
    std::vector<std::unique_ptr<DevaMachine>> _deva_machine;
};

} // namespace pain::deva::mock
