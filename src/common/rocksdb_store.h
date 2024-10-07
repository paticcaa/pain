#pragma once
#include "common/store.h"

namespace rocksdb {
class DB;
}
namespace pain::common {
class RocksdbStore;
using RocksdbStorePtr = boost::intrusive_ptr<RocksdbStore>;
class RocksdbStore : public Store {
public:
    RocksdbStore();
    ~RocksdbStore();

    static Status open(const char* data_path, RocksdbStorePtr* store);
    Status close();
    Status recover(const char* from);
    Status check_point(const char* to, std::vector<std::string>* files);

    Status hset(std::string_view key, std::string_view field, std::string_view value) override;
    Status hget(std::string_view key, std::string_view field, std::string* value) override;
    Status hdel(std::string_view key, std::string_view field) override;
    Status hlen(std::string_view key, size_t* len) override;
    std::shared_ptr<Iterator> hgetall(std::string_view key) override;
    bool hexists(std::string_view key, std::string_view field) override;

private:
    std::string make_key(std::string_view key, std::string_view field) const;
    void open_or_die();
    std::string _data_path;
    rocksdb::DB* _db;
};

} // namespace pain::common
