#pragma once

#include "pain/proto/asura.pb.h"
#include "asura/macro.h"
#include "common/store.h"

namespace pain::asura {

class AsuraServiceImpl : public pain::proto::asura::AsuraService {
public:
    AsuraServiceImpl(common::StorePtr store) : _store(store) {}
    ASURA_RPC_ENTRY(RegisterDeva);
    ASURA_RPC_ENTRY(ListDeva);

private:
    common::StorePtr _store;
};

} // namespace pain::asura
