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

// Initial control-plane messages
#pragma pack(push, 1)
struct InfoMsg {
    char tag[4];           // "INFO"
    uint32_t chunk_bytes;  // payload bytes per DATA
    uint64_t file_size;    // total file size in bytes
    uint32_t chunk_count;  // total number of chunks
    uint64_t session_id;   // random session id
};

struct InfoAck {
    char tag[4];           // "IACK"
    uint64_t session_id;
};

// Data plane frame header
struct DataMsg {
    char tag[4];   // "DATA"
    uint32_t seq;  // chunk index
    uint16_t payload; // bytes that follow
};
#pragma pack(pop)
