#include "common/rocksdb_store.h"
#include <braft/file_system_adaptor.h>
#include <pain/base/plog.h>
#include <pain/base/scope_exit.h>
#include <pain/base/types.h>
#include <rocksdb/db.h>
#include <rocksdb/utilities/checkpoint.h>
#include <string>
#include <boost/assert.hpp>

namespace pain::common {

RocksdbStore::RocksdbStore() {}

RocksdbStore::~RocksdbStore() {
    auto status = close();
    if (!status.ok()) {
        PLOG_ERROR(("desc", "close rocksdb failed") //
                   ("error", status.error_str()));
    }
}

Status RocksdbStore::open(const char* data_path, RocksdbStorePtr* store) {
    BOOST_ASSERT(data_path != nullptr);
    BOOST_ASSERT(store != nullptr);
    auto fs = braft::default_file_system();

    if (!fs->directory_exists(data_path)) {
        butil::File::Error error = butil::File::FILE_OK;
        if (!fs->create_directory(data_path, &error, true)) {
            if (error != butil::File::FILE_OK) {
                PLOG_ERROR(("desc", "create dir failed") //
                           ("path", data_path)           //
                           ("error", static_cast<int>(error)));
            }
            return Status(EIO, "create dir %s failed", data_path);
        }
    }
    rocksdb::Options options;
    options.create_if_missing = true;
    PLOG_INFO(("desc", "open rocksdb") //
              ("path", data_path));

    rocksdb::DB* db = nullptr;
    rocksdb::Status status = rocksdb::DB::Open(options, data_path, &db);
    if (!status.ok()) {
        PLOG_ERROR(("desc", "open rocksdb failed") //
                   ("path", data_path)("error", status.ToString()));
        return Status(EIO, status.ToString());
    }

    RocksdbStorePtr rocksdb_store = new RocksdbStore();
    rocksdb_store->_data_path = data_path;
    rocksdb_store->_db = db;
    *store = rocksdb_store;
    return Status::OK();
}

void RocksdbStore::open_or_die() {
    rocksdb::Options options;
    rocksdb::Status status = rocksdb::DB::Open(options, _data_path, &_db);
    if (!status.ok()) {
        PLOG_ERROR(("desc", "open rocksdb failed") //
                   ("path", _data_path)("error", status.ToString()));
        BOOST_ASSERT_MSG(false, status.ToString().c_str());
    }
}

Status RocksdbStore::close() {
    if (_db == nullptr) {
        return Status::OK();
    }
    PLOG_INFO(("desc", "close rocksdb") //
              ("path", _data_path));
    auto status = _db->Close();
    if (!status.ok()) {
        PLOG_ERROR(("desc", "close rocksdb failed") //
                   ("path", _data_path)("error", status.ToString()));
        return Status(EIO, status.ToString());
    }
    delete _db;
    _db = nullptr;
    return Status::OK();
}

Status RocksdbStore::check_point(const char* to, std::vector<std::string>* files) {
    BOOST_ASSERT(to != nullptr);
    BOOST_ASSERT(files != nullptr);
    rocksdb::FlushOptions options;
    options.wait = true;
    options.allow_write_stall = false;
    auto st = _db->Flush(options);
    if (!st.ok()) {
        PLOG_ERROR(("desc", "flush rocksdb failed") //
                   ("error", st.ToString()));
        return Status(EIO, st.ToString());
    }

    rocksdb::Checkpoint* cpt = nullptr;
    rocksdb::Status status = rocksdb::Checkpoint::Create(_db, &cpt);
    if (!status.ok()) {
        PLOG_ERROR(("desc", "create checkpoint failed") //
                   ("error", status.ToString()));
        return Status(EIO, status.ToString());
    }
    std::unique_ptr<rocksdb::Checkpoint> cpt_guard(cpt);

    status = cpt->CreateCheckpoint(to);
    if (!status.ok()) {
        PLOG_ERROR(("desc", "create checkpoint failed") //
                   ("error", status.ToString()));
        return Status(EIO, status.ToString());
    }

    auto fs = braft::default_file_system();
    std::unique_ptr<braft::DirReader> dir_reader(fs->directory_reader(to));

    if (dir_reader == nullptr) {
        PLOG_ERROR(("desc", "fail to open cpt dir") //
                   ("path", to));
        BOOST_ASSERT_MSG(false, "fail to open cpt dir");
    }

    if (!dir_reader->is_valid()) {
        PLOG_ERROR(("desc", "cpt dir is invalid") //
                   ("path", to));
        BOOST_ASSERT_MSG(false, "cpt dir is invalid");
    }

    std::vector<std::string> snapshot_files;
    while (dir_reader->next()) {
        auto file_name = fmt::format("cpt/{}", dir_reader->name());
        PLOG_INFO(("desc", "snapshot add file") //
                  ("file", file_name));
        snapshot_files.push_back(file_name);
    }

    files->swap(snapshot_files);
    return Status::OK();
}

Status RocksdbStore::recover(const char* from) {
    BOOST_ASSERT(from != nullptr);
    PLOG_INFO(("desc", "recover rocksdb") //
              ("from", from));
    auto status = close();
    if (!status.ok()) {
        return status;
    }

    auto rollback_open = make_scope_exit([&] {
        open_or_die();
    });

    auto fs = braft::default_file_system();
    auto bak_path = _data_path + ".bak";
    if (fs->path_exists(bak_path) && !fs->delete_file(bak_path, true)) {
        PLOG_ERROR(("desc", "delete bak failed") //
                   ("path", bak_path));
        return Status(EIO, "remove bak dir failed");
    }
    if (!fs->rename(_data_path, bak_path)) {
        PLOG_ERROR(("desc", "rename failed") //
                   ("src", _data_path)       //
                   ("dest", bak_path));
        return Status(EIO, "rename failed");
    }

    auto rollback_dir = make_scope_exit([&] {
        if (!fs->rename(bak_path, _data_path)) {
            PLOG_ERROR(("desc", "rollback failed") //
                       ("src", bak_path)           //
                       ("dest", _data_path));
            BOOST_ASSERT_MSG(false, "rollbak rename failed");
        }
    });

    rocksdb::Options options;
    rocksdb::DB* db = nullptr;
    rocksdb::Status st = rocksdb::DB::OpenForReadOnly(options, from, &db);
    std::unique_ptr<rocksdb::DB> db_guard(db);

    if (!st.ok()) {
        PLOG_ERROR(("desc", "recover rocksdb failed") //
                   ("from", from)("error", st.ToString()));
        return Status(EIO, st.ToString());
    }

    rocksdb::Checkpoint* cpt = nullptr;
    st = rocksdb::Checkpoint::Create(db, &cpt);
    if (!st.ok()) {
        PLOG_ERROR(("desc", "create checkpoint failed") //
                   ("error", st.ToString()));
        return Status(EIO, st.ToString());
    }
    std::unique_ptr<rocksdb::Checkpoint> cpt_guard(cpt);

    st = cpt->CreateCheckpoint(_data_path);
    if (!st.ok()) {
        PLOG_ERROR(("desc", "create checkpoint failed") //
                   ("error", st.ToString()));
        return Status(EIO, st.ToString());
    }

    rollback_dir.release();
    rollback_open.release();

    if (!fs->delete_file(bak_path, true)) {
        // Only log here.
        PLOG_ERROR(("desc", "delete bak failed") //
                   ("path", bak_path));
    }

    open_or_die();

    return Status::OK();
}

std::string RocksdbStore::make_key(std::string_view key, std::string_view field) const {
    return std::format("{}_{}", key, field);
}

Status RocksdbStore::hset(std::string_view key, std::string_view field, std::string_view value) {
    rocksdb::WriteOptions options;
    options.disableWAL = true;
    options.sync = false;
    rocksdb::Status status = _db->Put(options, make_key(key, field), value);
    if (!status.ok()) {
        PLOG_ERROR(("desc", "hset failed") //
                   ("key", key)("field", field)("error", status.ToString()));
        return Status(EIO, status.ToString());
    }
    return Status::OK();
}

Status RocksdbStore::hget(std::string_view key, std::string_view field, std::string* value) {
    rocksdb::ReadOptions options;
    rocksdb::Status status = _db->Get(options, make_key(key, field), value);
    if (!status.ok()) {
        PLOG_ERROR(("desc", "hget failed") //
                   ("key", key)("field", field)("error", status.ToString()));
        return Status(EIO, status.ToString());
    }
    return Status::OK();
}

Status RocksdbStore::hdel(std::string_view key, std::string_view field) {
    rocksdb::WriteOptions options;
    options.disableWAL = true;
    options.sync = false;
    rocksdb::Status status = _db->Delete(options, make_key(key, field));
    if (!status.ok()) {
        PLOG_ERROR(("desc", "hdel failed") //
                   ("key", key)("field", field)("error", status.ToString()));
        return Status(EIO, status.ToString());
    }
    return Status::OK();
}

Status RocksdbStore::hlen(std::string_view key, size_t* len) {
    auto it = hgetall(key);
    size_t size = 0;
    while (it->valid()) {
        it->next();
        ++size;
    }
    *len = size;
    return Status::OK();
}

class RocksdbStoreIterator : public RocksdbStore::Iterator {
public:
    RocksdbStoreIterator(rocksdb::Iterator* iter, std::string_view prefix) : _iter(iter), _prefix(prefix) {}
    ~RocksdbStoreIterator() override {
        delete _iter;
    }

    bool valid() override {
        return _iter->Valid() && _iter->key().starts_with(_prefix);
    }

    std::string_view key() override {
        return std::string_view(_iter->key().data(), _iter->key().size());
    }

    std::string_view value() override {
        return std::string_view(_iter->value().data(), _iter->value().size());
    }

    void next() override {
        _iter->Next();
    }

private:
    rocksdb::Iterator* _iter;
    std::string _prefix;
};

std::shared_ptr<RocksdbStore::Iterator> RocksdbStore::hgetall(std::string_view key) {
    rocksdb::ReadOptions options;
    rocksdb::Iterator* iter = _db->NewIterator(options);
    iter->Seek(key);
    return std::make_shared<RocksdbStoreIterator>(iter, key);
}

bool RocksdbStore::hexists(std::string_view key, std::string_view field) {
    std::string value;
    auto status = hget(key, field, &value);
    return status.ok();
}

} // namespace pain::common
