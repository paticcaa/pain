#pragma once

#include <braft/raft.h>    // braft::Node braft::StateMachine
#include <braft/storage.h> // braft::SnapshotWriter
#include <boost/intrusive_ptr.hpp>
#include "common/rsm/rsm.h"

namespace pain::deva {

common::RsmPtr default_rsm();

} // namespace pain::deva
