#pragma once

#include <pain/base/object_id.h>
#include "pain/proto/common.pb.h"

namespace pain::common {
inline proto::UUID to_proto(const UUID& uuid) {
    proto::UUID proto;
    proto.set_low(uuid.low());
    proto.set_high(uuid.high());
    return proto;
}

inline void to_proto(const UUID& uuid, proto::UUID* proto) {
    proto->set_low(uuid.low());
    proto->set_high(uuid.high());
}

inline UUID from_proto(const proto::UUID& proto) {
    return UUID(proto.high(), proto.low());
}

inline proto::ObjectId to_proto(const ObjectId& obj) {
    proto::ObjectId proto;
    proto.set_partition_id(obj.partition_id());
    to_proto(obj.uuid(), proto.mutable_uuid());
    return proto;
}

inline void to_proto(const ObjectId& obj, proto::ObjectId* proto) {
    proto->set_partition_id(obj.partition_id());
    to_proto(obj.uuid(), proto->mutable_uuid());
}

inline ObjectId from_proto(const proto::ObjectId& proto) {
    return ObjectId(proto.partition_id(), from_proto(proto.uuid()));
}

} // namespace pain::common

inline bool operator==(const pain::proto::ObjectId& lhs, const pain::proto::ObjectId& rhs) {
    return lhs.partition_id() == rhs.partition_id() && lhs.uuid().high() == rhs.uuid().high() &&
           lhs.uuid().low() == rhs.uuid().low();
}

inline bool operator==(const pain::proto::ObjectId& lhs, const pain::ObjectId& rhs) {
    return lhs.partition_id() == rhs.partition_id() && lhs.uuid().high() == rhs.uuid().high() &&
           lhs.uuid().low() == rhs.uuid().low();
}

inline bool operator==(const pain::ObjectId& lhs, const pain::proto::ObjectId& rhs) {
    return lhs.partition_id() == rhs.partition_id() && lhs.uuid().high() == rhs.uuid().high() &&
           lhs.uuid().low() == rhs.uuid().low();
}
