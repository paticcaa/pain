#include <gtest/gtest.h>
#include "pain/base/scope_exit.h"
#include "pain/proto/deva.pb.h"
#include "deva/mock/mock_deva.h"
#include "deva/sdk/rpc_client.h"

namespace {

class TestDeva : public ::testing::Test {
public:
    pain::Status mkdir(const std::string& path, pain::proto::deva::MkdirResponse* response) {
        pain::proto::deva::MkdirRequest request;
        request.set_path(path);
        return pain::deva::call_rpc(
            _mock_deva.group().c_str(), &pain::proto::deva::DevaService::Mkdir, &request, response);
    }

    pain::Status
    open(const std::string& path, pain::proto::deva::OpenFlag flags, pain::proto::deva::OpenFileResponse* response) {
        pain::proto::deva::OpenFileRequest request;
        request.set_path(path);
        request.set_flags(flags);
        return pain::deva::call_rpc(
            _mock_deva.group().c_str(), &pain::proto::deva::DevaService::OpenFile, &request, response);
    }

    pain::Status readdir(const std::string& path, pain::proto::deva::ReadDirResponse* response) {
        pain::proto::deva::ReadDirRequest request;
        request.set_path(path);
        return pain::deva::call_rpc(
            _mock_deva.group().c_str(), &pain::proto::deva::DevaService::ReadDir, &request, response);
    }

    pain::Status manusya_heartbeat(const pain::UUID& uuid,
                                   const char* ip,
                                   int32_t port,
                                   pain::proto::deva::ManusyaHeartbeatResponse* response) {
        pain::proto::deva::ManusyaHeartbeatRequest request;
        auto manusya_id = request.mutable_manusya_registration()->mutable_manusya_id();
        manusya_id->mutable_uuid()->set_high(uuid.high());
        manusya_id->mutable_uuid()->set_low(uuid.low());
        manusya_id->set_ip(ip);
        manusya_id->set_port(port);
        return pain::deva::call_rpc(
            _mock_deva.group().c_str(), &pain::proto::deva::DevaService::ManusyaHeartbeat, &request, response);
    }

    pain::Status list_manusya(pain::proto::deva::ListManusyaResponse* response) {
        pain::proto::deva::ListManusyaRequest request;
        return pain::deva::call_rpc(
            _mock_deva.group().c_str(), &pain::proto::deva::DevaService::ListManusya, &request, response);
    }

    void TearDown() override {
        if (::testing::Test::HasFailure()) {
            _mock_deva.do_not_remove_data_path();
        }
    }

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

TEST_F(TestDeva, CreateFile) {
    _mock_deva.start();
    SCOPE_EXIT {
        _mock_deva.stop();
    };
    std::string leader;
    auto status = _mock_deva.wait_for_leader(&leader);
    EXPECT_TRUE(status.ok()) << status.error_str() << "(" << status.error_code() << ")";
    std::cout << "leader: " << leader << std::endl;

    pain::proto::deva::OpenFileResponse response;
    status = open("/test.txt", pain::proto::deva::OpenFlag::OPEN_CREATE, &response);

    EXPECT_TRUE(status.ok()) << status.error_str() << "(" << status.error_code() << ")";
    std::cout << "response: " << response.DebugString() << std::endl;
}

TEST_F(TestDeva, CreateDirectoryAndFile) {
    _mock_deva.start();
    SCOPE_EXIT {
        _mock_deva.stop();
    };
    std::string leader;
    auto status = _mock_deva.wait_for_leader(&leader);
    EXPECT_TRUE(status.ok()) << status.error_str() << "(" << status.error_code() << ")";
    std::cout << "leader: " << leader << std::endl;

    {
        pain::proto::deva::MkdirResponse mkdir_response;
        status = mkdir("/test", &mkdir_response);
        ASSERT_TRUE(status.ok()) << status.error_str() << "(" << status.error_code() << ")";
        std::cout << "mkdir response: " << mkdir_response.DebugString() << std::endl;
    }

    pain::proto::FileInfo file_info;
    {
        pain::proto::deva::OpenFileResponse response;
        status = open("/test/test.txt", pain::proto::deva::OpenFlag::OPEN_CREATE, &response);
        EXPECT_TRUE(status.ok()) << status.error_str() << "(" << status.error_code() << ")";
        std::cout << "response: " << response.DebugString() << std::endl;
        file_info.Swap(response.mutable_file_info());
    }

    {
        pain::proto::deva::OpenFileResponse response;
        status = open("/test/test.txt", pain::proto::deva::OpenFlag::OPEN_READ, &response);
        EXPECT_TRUE(status.ok()) << status.error_str() << "(" << status.error_code() << ")";
        std::cout << "response: " << response.DebugString() << std::endl;
        EXPECT_EQ(response.file_info().file_id().high(), file_info.file_id().high());
        EXPECT_EQ(response.file_info().file_id().low(), file_info.file_id().low());
    }
}

TEST_F(TestDeva, ReadDir) {
    _mock_deva.start();
    SCOPE_EXIT {
        _mock_deva.stop();
    };
    std::string leader;
    auto status = _mock_deva.wait_for_leader(&leader);
    EXPECT_TRUE(status.ok()) << status.error_str() << "(" << status.error_code() << ")";
    std::cout << "leader: " << leader << std::endl;

    pain::proto::deva::MkdirResponse mkdir_response;
    status = mkdir("/test", &mkdir_response);
    ASSERT_TRUE(status.ok()) << status.error_str() << "(" << status.error_code() << ")";
    std::cout << "mkdir response: " << mkdir_response.DebugString() << std::endl;

    for (int i = 0; i < 10; i++) { // NOLINT(readability-magic-numbers)
        pain::proto::deva::OpenFileResponse response;
        status = open(fmt::format("/test/test_file_{}.txt", i), pain::proto::deva::OpenFlag::OPEN_CREATE, &response);
        ASSERT_TRUE(status.ok()) << status.error_str() << "(" << status.error_code() << ")";
        std::cout << "response: " << response.DebugString() << std::endl;
    }

    for (int i = 0; i < 10; i++) { // NOLINT(readability-magic-numbers)
        pain::proto::deva::MkdirResponse response;
        status = mkdir(fmt::format("/test/test_dir_{}", i), &response);
        ASSERT_TRUE(status.ok()) << status.error_str() << "(" << status.error_code() << ")";
        std::cout << "mkdir response: " << response.DebugString() << std::endl;
    }

    pain::proto::deva::ReadDirResponse readdir_response;
    status = readdir("/test", &readdir_response);
    ASSERT_TRUE(status.ok()) << status.error_str() << "(" << status.error_code() << ")";
    std::cout << "readdir response: " << readdir_response.DebugString() << std::endl;
    ASSERT_EQ(readdir_response.entries_size(), 20);
    for (int i = 0; i < 10; i++) { // NOLINT(readability-magic-numbers)
        EXPECT_EQ(readdir_response.entries(i).name(), fmt::format("test_file_{}.txt", i));
        EXPECT_EQ(readdir_response.entries(i).type(), pain::proto::FileType::FILE_TYPE_FILE);
    }
    for (int i = 0; i < 10; i++) { // NOLINT(readability-magic-numbers)
        EXPECT_EQ(readdir_response.entries(i + 10).name(), fmt::format("test_dir_{}", i));
        EXPECT_EQ(readdir_response.entries(i + 10).type(), pain::proto::FileType::FILE_TYPE_DIRECTORY);
    }
}

TEST_F(TestDeva, Snapshot) {
    _mock_deva.start();
    SCOPE_EXIT {
        _mock_deva.stop();
    };
    std::string leader;
    auto status = _mock_deva.wait_for_leader(&leader);
    ASSERT_TRUE(status.ok()) << status.error_str() << "(" << status.error_code() << ")";
    std::cout << "leader: " << leader << std::endl;

    pain::proto::deva::MkdirResponse mkdir_response;
    status = mkdir("/test", &mkdir_response);
    ASSERT_TRUE(status.ok()) << status.error_str() << "(" << status.error_code() << ")";
    std::cout << "mkdir response: " << mkdir_response.DebugString() << std::endl;

    pain::proto::deva::OpenFileResponse response;
    status = open("/test/test.txt", pain::proto::deva::OpenFlag::OPEN_CREATE, &response);
    ASSERT_TRUE(status.ok()) << status.error_str() << "(" << status.error_code() << ")";
    std::cout << "response: " << response.DebugString() << std::endl;

    status = _mock_deva.snapshot();
    ASSERT_TRUE(status.ok()) << status.error_str() << "(" << status.error_code() << ")";
    std::cout << "snapshot response: " << status.error_str() << "(" << status.error_code() << ")" << std::endl;

    // restart the deva
    _mock_deva.stop();
    status = _mock_deva.start();
    ASSERT_TRUE(status.ok()) << status.error_str() << "(" << status.error_code() << ")";
    std::cout << "start response: " << status.error_str() << "(" << status.error_code() << ")" << std::endl;

    status = _mock_deva.wait_for_leader(&leader);
    ASSERT_TRUE(status.ok()) << status.error_str() << "(" << status.error_code() << ")";
    std::cout << "leader: " << leader << std::endl;

    pain::proto::deva::ReadDirResponse readdir_response;
    status = readdir("/test", &readdir_response);
    ASSERT_TRUE(status.ok()) << status.error_str() << "(" << status.error_code() << ")";
    std::cout << "readdir response: " << readdir_response.DebugString() << std::endl;
    ASSERT_EQ(readdir_response.entries_size(), 1);
    EXPECT_EQ(readdir_response.entries(0).name(), "test.txt");
    EXPECT_EQ(readdir_response.entries(0).type(), pain::proto::FileType::FILE_TYPE_FILE);
}

TEST_F(TestDeva, ListManusya) {
    _mock_deva.start();
    SCOPE_EXIT {
        _mock_deva.stop();
    };
    std::string leader;
    auto status = _mock_deva.wait_for_leader(&leader);
    ASSERT_TRUE(status.ok()) << status.error_str() << "(" << status.error_code() << ")";
    std::cout << "leader: " << leader << std::endl;

    {
        pain::proto::deva::ListManusyaResponse response;
        status = list_manusya(&response);
        ASSERT_TRUE(status.ok()) << status.error_str() << "(" << status.error_code() << ")";
        std::cout << "list_manusya response: " << response.DebugString() << std::endl;
    }

    {
        // heartbeat
        pain::proto::deva::ManusyaHeartbeatResponse response;
        pain::UUID uuid(1, 2);
        status = manusya_heartbeat(uuid, "127.0.0.1", 12345, &response); // NOLINT(readability-magic-numbers)
        ASSERT_TRUE(status.ok()) << status.error_str() << "(" << status.error_code() << ")";
        std::cout << "manusya_heartbeat response: " << response.DebugString() << std::endl;
    }

    {
        pain::proto::deva::ListManusyaResponse response;
        status = list_manusya(&response);
        ASSERT_TRUE(status.ok()) << status.error_str() << "(" << status.error_code() << ")";
        std::cout << "list_manusya response: " << response.DebugString() << std::endl;
        pain::UUID uuid(1, 2);
        ASSERT_EQ(response.manusya_descriptors_size(), 1);
        EXPECT_EQ(response.manusya_descriptors(0).manusya_id().uuid().high(), uuid.high());
        EXPECT_EQ(response.manusya_descriptors(0).manusya_id().uuid().low(), uuid.low());
        EXPECT_EQ(response.manusya_descriptors(0).manusya_id().ip(), "127.0.0.1");
        EXPECT_EQ(response.manusya_descriptors(0).manusya_id().port(), 12345);
        EXPECT_TRUE(response.manusya_descriptors(0).is_alive());
    }
}

} // namespace
