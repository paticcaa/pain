#pragma once

#include <boost/intrusive_ptr.hpp>
#include "pain/proto/deva_store.pb.h"
#include "base/types.h"
#include "deva/container.h"

#define DEVA_ENTRY(name)                                                                                               \
    Status name(const pain::proto::deva::store::name##Request* request,                                                \
                pain::proto::deva::store::name##Response* response,                                                    \
                int64_t index);                                                                                        \
    Status process(const pain::proto::deva::store::name##Request* request,                                             \
                   pain::proto::deva::store::name##Response* response,                                                 \
                   int64_t index) {                                                                                    \
        return name(request, response, index);                                                                         \
    }

namespace pain::deva {

class Deva;
using DevaPtr = boost::intrusive_ptr<Deva>;
class Deva : public Container {
public:
    DEVA_ENTRY(Open);
    DEVA_ENTRY(Close);
    DEVA_ENTRY(Remove);
    DEVA_ENTRY(Seal);
    DEVA_ENTRY(CreateChunk);
    DEVA_ENTRY(RemoveChunk);
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
