#pragma once

#include <boost/intrusive_ptr.hpp>
#include "pain/proto/deva_store.pb.h"
#include "base/types.h"
#include "deva/container.h"

#define DEVA_ENTRY(name)                                                                                               \
    Status name([[maybe_unused]] const pain::proto::deva::store::name##Request* request,                               \
                [[maybe_unused]] pain::proto::deva::store::name##Response* response,                                   \
                [[maybe_unused]] int64_t index);                                                                       \
    Status process([[maybe_unused]] const pain::proto::deva::store::name##Request* request,                            \
                   [[maybe_unused]] pain::proto::deva::store::name##Response* response,                                \
                   [[maybe_unused]] int64_t index) {                                                                   \
        return name(request, response, index);                                                                         \
    }

namespace pain::deva {

class Deva;
using DevaPtr = boost::intrusive_ptr<Deva>;
class Deva : public Container {
public:
    DEVA_ENTRY(CreateFile);
    DEVA_ENTRY(RemoveFile);
    DEVA_ENTRY(SealFile);
    DEVA_ENTRY(CreateChunk);
    DEVA_ENTRY(CheckInChunk);
    DEVA_ENTRY(SealChunk);
    DEVA_ENTRY(SealAndNewChunk);

    Status save_snapshot(std::string_view path, std::vector<std::string>* files) override;
    Status load_snapshot(std::string_view path) override;

private:
    std::atomic<int> _use_count = {};

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
