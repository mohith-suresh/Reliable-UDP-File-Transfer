// Minimal common definitions (initial scaffolding)
#pragma once

#include <cstdint>
#include <cstring>

// 4-byte ASCII tag helper
struct Tag {
    char v[4];
    explicit Tag(const char* s) { std::memcpy(v, s, 4); }
};

inline bool tagEq(const char* a, const char* b) {
    return std::memcmp(a, b, 4) == 0;
}

// Early constant: conservative UDP payload
static constexpr int DEFAULT_CHUNK = 1460;
