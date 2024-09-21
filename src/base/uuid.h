#pragma once

#include <uuid_v4/uuid_v4.h>
#include <optional>

namespace pain {

class UUID : public UUIDv4::UUID {
public:
    using UUIDv4::UUID::UUID;

    UUID(const UUIDv4::UUID& uuid) : UUIDv4::UUID(uuid) {}

    uint64_t low() const {
        const char* _data = reinterpret_cast<const char*>(this);
        return *reinterpret_cast<const uint64_t*>(_data);
    }

    uint64_t high() const {
        const char* _data = reinterpret_cast<const char*>(this);
        return *reinterpret_cast<const uint64_t*>(_data + 8);
    }

    static bool valid(const std::string& uuid) {
        const size_t kGUIDLength = 36U;
        if (uuid.length() != kGUIDLength)
            return false;

        const std::string hexchars = "0123456789abcdef";
        for (uint32_t i = 0; i < uuid.length(); ++i) {
            char current = uuid[i];
            if (i == 8 || i == 13 || i == 18 || i == 23) {
                if (current != '-')
                    return false;
            } else {
                if (hexchars.find(current) == std::string::npos)
                    return false;
            }
        }

        return true;
    }

    static std::optional<UUID> from_str(const std::string& str) {
        if (!valid(str)) {
            return std::nullopt;
        }

        auto uuid = UUIDv4::UUID::fromStrFactory(str);
        return UUID(uuid);
    }

    static UUID from_str_or_die(const std::string& str) {
        auto uuid = UUIDv4::UUID::fromStrFactory(str);
        return UUID(uuid);
    }
};
} // namespace pain
