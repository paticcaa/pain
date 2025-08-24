#include <gtest/gtest.h>
#include "pain/base/scope_exit.h"
#include "deva/mock/mock_deva.h"
#include "deva/sdk/rpc_client.h"

namespace {

class TestDeva : public ::testing::Test {
protected:
    pain::deva::mock::MockDeva _mock_deva;
};

TEST_F(TestDeva, Basic) {
    _mock_deva.start();
    SCOPE_EXIT {
        _mock_deva.stop();
    };
    std::string leader;
    auto status = _mock_deva.wait_for_leader(&leader);
    EXPECT_TRUE(status.ok()) << status.error_str() << "(" << status.error_code() << ")";
    std::cout << "leader: " << leader << std::endl;
    auto it = std::find_if(
        _mock_deva.node_addrs().begin(), _mock_deva.node_addrs().end(), [&leader](const std::string& addr) {
            return leader.find(addr) != std::string::npos;
        });
    EXPECT_NE(it, _mock_deva.node_addrs().end()) << "leader: " << leader;
}

TEST_F(TestDeva, StartTwoNodes) {
    _mock_deva.start(0);
    _mock_deva.start(1);

    SCOPE_EXIT {
        _mock_deva.stop(0);
        _mock_deva.stop(1);
    };
    std::string leader;
    auto status = _mock_deva.wait_for_leader(&leader);
    EXPECT_TRUE(status.ok()) << status.error_str() << "(" << status.error_code() << ")";
    std::cout << "leader: " << leader << std::endl;
    auto it = std::find_if(
        _mock_deva.node_addrs().begin(), _mock_deva.node_addrs().end(), [&leader](const std::string& addr) {
            return leader.find(addr) != std::string::npos;
        });
    EXPECT_NE(it, _mock_deva.node_addrs().end()) << "leader: " << leader;
}

} // namespace
