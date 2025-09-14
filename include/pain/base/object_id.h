#pragma once

#include <pain/base/uuid.h>
#include <string>
#include <fmt/format.h>
#include <boost/assert.hpp>

namespace pain {

class ObjectId {
public:
    ObjectId(uint32_t partition_id, UUID uuid) : _reserved(0), _partition_id(partition_id), _uuid(uuid) {}

    uint32_t partition_id() const {
        return _partition_id;
    }

    UUID uuid() const {
        return _uuid;
    }

    bool operator==(const ObjectId& other) const {
        return _partition_id == other._partition_id && _uuid == other._uuid;
    }

    bool operator!=(const ObjectId& other) const {
        return !(*this == other);
    }

    bool operator<(const ObjectId& other) const {
        return _partition_id < other._partition_id || (_partition_id == other._partition_id && _uuid < other._uuid);
    }

    bool operator>(const ObjectId& other) const {
        return other < *this;
    }

    bool operator<=(const ObjectId& other) const {
        return !(*this > other);
    }

    bool operator>=(const ObjectId& other) const {
        return !(*this < other);
    }

    std::string to_string() const {
        return fmt::format("{}-{}", _partition_id, _uuid.str());
    }

    static std::optional<ObjectId> from_str(std::string_view str) {
        auto pos = str.find('-');
        if (pos == std::string::npos) {
            return std::nullopt;
        }
        auto [partition_id_str, uuid_str] = split(str);
        if (partition_id_str.empty() || uuid_str.empty()) {
            return std::nullopt;
        }
        uint32_t pid = 0;
        try {
            pid = std::stoull(partition_id_str.data());
        } catch (const std::exception& e) {
            return std::nullopt;
        }
        auto uuid = UUID::from_str(uuid_str);
        if (!uuid) {
            return std::nullopt;
        }
        return ObjectId(pid, uuid.value());
    }

    static ObjectId from_str_or_die(std::string_view str) {
        auto [partition_id_str, uuid_str] = split(str);
        if (partition_id_str.empty() || uuid_str.empty()) {
            BOOST_ASSERT_MSG(false, fmt::format("Invalid ObjectId string: {}", str).c_str());
        }
        uint32_t pid = 0;
        try {
            pid = std::stoull(partition_id_str.data());
        } catch (const std::exception& e) {
            BOOST_ASSERT_MSG(false, fmt::format("Invalid ObjectId string: {}", str).c_str());
        }

        auto uuid = UUID::from_str_or_die(uuid_str);
        return ObjectId(pid, uuid);
    }

private:
    static std::pair<std::string_view, std::string_view> split(std::string_view str) {
        auto pos = str.find('-');
        BOOST_ASSERT_MSG(pos != std::string::npos, "Invalid ObjectId string");
        return {str.substr(0, pos), str.substr(pos + 1)};
    }

private:
    uint32_t _reserved;
    uint32_t _partition_id;
    UUID _uuid;
};

} // namespace pain

namespace std {
template <>
struct hash<pain::ObjectId> {
    std::size_t operator()(const pain::ObjectId& obj) const {
        return std::hash<uint64_t>()(obj.partition_id()) ^ std::hash<pain::UUID>()(obj.uuid());
    }
};
} // namespace std
