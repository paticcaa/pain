#include "deva/mock/mock_deva.h"
#include <braft/route_table.h>
#include <pain/base/path.h>
#include <pain/base/plog.h>
#include <cerrno>
#include <fmt/format.h>
#include <fmt/ranges.h>

namespace pain::deva::mock {

MockDeva::MockDeva() :
    _group("test_group"),
    _data_path("/tmp/deva_mock_data_XXXXXX"),
    _node_addrs({"127.0.0.1:8200", "127.0.0.1:8201", "127.0.0.1:8202"}) {
    _node_conf = fmt::format("{}", fmt::join(_node_addrs, ","));
    make_temp_dir_or_die(&_data_path);
    PLOG_INFO(("data_path", _data_path));

    auto node_size = _node_addrs.size();

    _deva_machine.resize(node_size);
    _data_paths.resize(node_size);

    for (size_t i = 0; i < node_size; i++) {
        _data_paths[i] = fmt::format("{}/{}", _data_path, i);
    }
    braft::rtb::update_configuration(_group, _node_conf);
}

MockDeva::~MockDeva() {
    stop();
    std::filesystem::remove_all(_data_path);
}

Status MockDeva::start() {
    for (size_t i = 0; i < _deva_machine.size(); i++) {
        auto status = start(i);
        if (!status.ok()) {
            return status;
        }
    }
    return Status::OK();
}

Status MockDeva::start(int index) {
    _deva_machine[index] = std::make_unique<DevaMachine>(
        _data_paths[index].c_str(), _group.c_str(), _node_addrs[index].c_str(), _node_conf.c_str());
    return _deva_machine[index]->start();
}

void MockDeva::stop(int index) {
    if (_deva_machine[index]) {
        _deva_machine[index]->stop();
        _deva_machine[index].reset();
    }
}

void MockDeva::stop() {
    for (size_t i = 0; i < _deva_machine.size(); i++) {
        stop(i);
    }
}

Status MockDeva::wait_for_leader(std::string* addr, int timeout_ms) {
    constexpr int sleep_interval_ms = 500;
    for (int i = 0; i < timeout_ms / sleep_interval_ms; i++) {
        auto status = braft::rtb::refresh_leader(_group, sleep_interval_ms);
        if (!status.ok()) {
            bthread_usleep(sleep_interval_ms * 1000); // NOLINT(readability-magic-numbers)
            continue;
        }
        braft::PeerId leader;
        braft::rtb::select_leader(_group, &leader);
        if (leader.is_empty()) {
            bthread_usleep(sleep_interval_ms * 1000); // NOLINT(readability-magic-numbers)
            continue;
        }
        *addr = leader.to_string();
        return Status::OK();
    }
    return Status(ENODEV, "No leader");
}

} // namespace pain::deva::mock
