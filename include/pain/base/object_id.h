#pragma once

#include <pain/base/uuid.h>
#include <string>
#include <fmt/format.h>
#include <boost/assert.hpp>

namespace pain {

class ObjectId {
public:
    ObjectId() : _reserved(0), _partition_id(0) {}
    ObjectId(uint32_t partition_id, UUID uuid) : _reserved(0), _partition_id(partition_id), _uuid(uuid) {}

    uint32_t partition_id() const {
        return _partition_id;
    }

    UUID uuid() const {
        return _uuid;
    }

    static bool valid(std::string_view str) {
        // such as: 00000000-73404092-a3c7-471c-8364-10e96c1dada1
        constexpr size_t length = 45U;
        if (str.length() != length) {
            return false;
        }

        const std::string hexchars = "0123456789abcdef";
        for (uint32_t i = 0; i < str.length(); ++i) {
            char current = str[i];
            if (i == 8 || i == 17 || i == 22 || i == 27 || i == 32) { // NOLINT(readability-magic-numbers)
                if (current != '-') {
                    return false;
                }
            } else {
                if (hexchars.find(current) == std::string::npos) {
                    return false;
                }
            }
        }

        return true;
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
        return fmt::format("{:08x}-{}", _partition_id, _uuid.str());
    }

    std::string str() const {
        return to_string();
    }

    friend std::ostream& operator<<(std::ostream& os, const ObjectId& obj) {
        return os << obj.str();
    }

    static std::optional<ObjectId> from_str(std::string_view str) {
        auto pos = str.find('-');
        if (pos == std::string::npos) {
            return std::nullopt;
        }
        auto [partition_id_str, uuid_str] = split(str);
        if (partition_id_str.size() != 8UL || uuid_str.size() != 36UL) { // NOLINT
            return std::nullopt;
        }
        uint32_t pid = 0;
        try {
            size_t pos = 0;
            pid = std::stoull(partition_id_str.data(), &pos, 16); // NOLINT
            BOOST_ASSERT_MSG(pos == partition_id_str.size(), fmt::format("Invalid ObjectId string: {}", str).c_str());
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
        if (partition_id_str.size() != 8UL || uuid_str.size() != 36UL) { // NOLINT
            BOOST_ASSERT_MSG(false, fmt::format("Invalid ObjectId string: {}", str).c_str());
        }
        uint32_t pid = 0;
        try {
            size_t pos = 0;
            pid = std::stoull(partition_id_str.data(), &pos, 16); // NOLINT
            BOOST_ASSERT_MSG(pos == partition_id_str.size(), fmt::format("Invalid ObjectId string: {}", str).c_str());
        } catch (const std::exception& e) {
            BOOST_ASSERT_MSG(false, fmt::format("Invalid ObjectId string: {}", str).c_str());
        }

        auto uuid = UUID::from_str_or_die(uuid_str);
        return ObjectId(pid, uuid);
    }

    static ObjectId generate(uint32_t partition_id) {
        return ObjectId(partition_id, UUID::generate());
    }

private:
    static std::pair<std::string_view, std::string_view> split(std::string_view str) {
        auto pos = str.find('-');
        BOOST_ASSERT_MSG(pos != std::string::npos, fmt::format("Invalid ObjectId string: {}", str).c_str());
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
