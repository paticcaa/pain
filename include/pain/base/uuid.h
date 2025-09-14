#pragma once

#include <uuid_v4/uuid_v4.h>
#include <functional>
#include <optional>
#include <fmt/format.h>
#include <boost/assert.hpp>

namespace pain {

class UUID : public UUIDv4::UUID {
public:
    using UUIDv4::UUID::UUID;

    UUID(const UUIDv4::UUID& uuid) : UUIDv4::UUID(uuid) {}

    uint64_t low() const {
        const char* data = reinterpret_cast<const char*>(this);
        return *reinterpret_cast<const uint64_t*>(data);
    }

    uint64_t high() const {
        const char* data = reinterpret_cast<const char*>(this);
        return *reinterpret_cast<const uint64_t*>(data + 8); // NOLINT
    }

    static bool valid(std::string_view uuid) {
        constexpr size_t guid_length = 36U;
        if (uuid.length() != guid_length) {
            return false;
        }

        const std::string hexchars = "0123456789abcdef";
        for (uint32_t i = 0; i < uuid.length(); ++i) {
            char current = uuid[i];
            if (i == 8 || i == 13 || i == 18 || i == 23) { // NOLINT(readability-magic-numbers)
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

    static std::optional<UUID> from_str(std::string_view str) {
        if (!valid(str)) {
            return std::nullopt;
        }

        auto uuid = UUIDv4::UUID::fromStrFactory(str.data());
        return UUID(uuid);
    }

    static UUID from_str_or_die(std::string_view str) {
        BOOST_ASSERT_MSG(valid(str), fmt::format("Invalid UUID string: {}", str).c_str());
        auto uuid = UUIDv4::UUID::fromStrFactory(str.data());
        return UUID(uuid);
    }

    static UUID generate() {
        static thread_local UUIDv4::UUIDGenerator<std::mt19937_64> s_uuid_gen;
        return UUID(s_uuid_gen.getUUID());
    }
};

} // namespace pain

// std::hash specialization for pain::UUID
namespace std {
template <>
struct hash<pain::UUID> {
    std::size_t operator()(const pain::UUID& uuid) const noexcept {
        // Use the low and high parts of the UUID for hashing
        // This provides a good distribution and is consistent with the UUID structure
        std::size_t h1 = std::hash<uint64_t>{}(uuid.low());
        std::size_t h2 = std::hash<uint64_t>{}(uuid.high());
        return h1 ^ (h2 << 1);
    }
};
} // namespace std
