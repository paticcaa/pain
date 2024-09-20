#pragma once

#include <brpc/controller.h>
#include <json2pb/pb_to_json.h>
#include <nlohmann/json.hpp>
#include "base/types.h"

using json = nlohmann::ordered_json;

namespace pain::sad {
json pb_to_json(const google::protobuf::Message& message);
void print(const Status& status);
void print(const brpc::Controller& cntl,
           const google::protobuf::Message* message = nullptr,
           std::function<void(json&)> f = nullptr);
std::string format_command(const std::string& name);
} // namespace pain::sad
