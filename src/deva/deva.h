#pragma once

#include <pain/base/plog.h>
#include <pain/base/types.h>
#include <boost/intrusive_ptr.hpp>
#include "pain/proto/deva_store.pb.h"
#include "common/store.h"
#include "common/txn_manager.h"
#include "common/txn_store.h"
#include "deva/container.h"
#include "deva/manusya_descriptor.h"
#include "deva/namespace.h"
#include "deva/op.h"

#define DEVA_ENTRY(name)                                                                                               \
    Status name([[maybe_unused]] int32_t version,                                                                      \
                [[maybe_unused]] const pain::proto::deva::store::name##Request* request,                               \
                [[maybe_unused]] pain::proto::deva::store::name##Response* response,                                   \
                [[maybe_unused]] int64_t index);                                                                       \
    Status process([[maybe_unused]] int32_t version,                                                                   \
                   [[maybe_unused]] const pain::proto::deva::store::name##Request* request,                            \
                   [[maybe_unused]] pain::proto::deva::store::name##Response* response,                                \
                   [[maybe_unused]] int64_t index) {                                                                   \
        if (!need_apply(OpType::k##name)) {                                                                            \
            return name(version, request, response, index);                                                            \
        }                                                                                                              \
        if (check_index_is_applied(index)) {                                                                           \
            PLOG_INFO(("desc", "index is applied already")("index", index));                                           \
            return Status::OK();                                                                                       \
        }                                                                                                              \
        auto txn = _store->begin_txn();                                                                                \
        auto status = Status::OK();                                                                                    \
        PAIN_TXN(txn.get()) {                                                                                          \
            status = set_applied_index(index);                                                                         \
            if (!status.ok()) {                                                                                        \
                PLOG_ERROR(("desc", "set applied index failed")("index", index)("error", status.error_str()));         \
                return status;                                                                                         \
            }                                                                                                          \
            status = name(version, request, response, index);                                                          \
            return status;                                                                                             \
        };                                                                                                             \
        return status;                                                                                                 \
    }

namespace pain::deva {

class Deva;
using DevaPtr = boost::intrusive_ptr<Deva>;
class Deva : public Container {
public:
    Deva(common::StorePtr store) : _store(store), _namespace(store) {}

    DEVA_ENTRY(CreateFile);
    DEVA_ENTRY(CreateDir);
    DEVA_ENTRY(ReadDir);
    DEVA_ENTRY(RemoveFile);
    DEVA_ENTRY(SealFile);
    DEVA_ENTRY(CreateChunk);
    DEVA_ENTRY(CheckInChunk);
    DEVA_ENTRY(SealChunk);
    DEVA_ENTRY(SealAndNewChunk);
    DEVA_ENTRY(GetFileInfo);
    DEVA_ENTRY(ManusyaHeartbeat);
    DEVA_ENTRY(ListManusya);

    Status save_snapshot(std::string_view path, std::vector<std::string>* files) override;
    Status load_snapshot(std::string_view path) override;

private:
    Status create(const std::string& path, const UUID& id, FileType type);
    Status set_applied_index(int64_t applied_index);
    bool check_index_is_applied(int64_t index) const {
        return index != 0 && index <= _applied_index;
    }

    Status update_file_info(const UUID& id, const proto::FileInfo& file_info);
    Status get_file_info(const UUID& id, proto::FileInfo* file_info);
    Status remove_file_info(const UUID& id);

private:
    std::atomic<int> _use_count;
    common::StorePtr _store;
    Namespace _namespace;
    const char* _file_info_key = "file_info";
    const char* _meta_key = "meta";
    const char* _applied_index_key = "applied_index";
    int64_t _applied_index = 0;

    // don't need persist
    std::unordered_map<UUID, ManusyaDescriptor> _manusya_descriptors;

    friend void intrusive_ptr_add_ref(Deva* deva) {
        ++deva->_use_count;
    }

    friend void intrusive_ptr_release(Deva* deva) {
        if (deva->_use_count.fetch_sub(1) == 1) {
            delete deva;
        }
    }
};

} // namespace pain::deva
