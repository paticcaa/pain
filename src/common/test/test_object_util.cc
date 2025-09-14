#include <gtest/gtest.h>
#include "pain/proto/common.pb.h"
#include "common/object_id_util.h"

// NOLINTBEGIN(readability-magic-numbers)

namespace {
using namespace pain;
using namespace pain::common;

TEST(ObjectIdUtil, ToProto) {
    ObjectId obj(123, UUID::from_str_or_die("050e8400-0000-0000-0000-000000000000"));
    auto proto = to_proto(obj);
    EXPECT_EQ(proto.partition_id(), 123);
    EXPECT_EQ(proto.uuid().high(), 0);
    EXPECT_EQ(proto.uuid().low(), 0x840e05);
}

TEST(ObjectIdUtil, FromProto) {
    proto::ObjectId proto;
    proto.set_partition_id(123);
    to_proto(UUID::from_str_or_die("050e8400-0000-0000-0000-000000000000"), proto.mutable_uuid());
    auto obj = from_proto(proto);
    EXPECT_EQ(obj.partition_id(), 123);
    EXPECT_EQ(obj.uuid().str(), "050e8400-0000-0000-0000-000000000000");
}
} // namespace

// NOLINTEND(readability-magic-numbers)
