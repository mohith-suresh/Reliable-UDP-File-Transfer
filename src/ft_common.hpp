// Minimal common definitions for the fresh UDP file transfer protocol
#pragma once

#include <cstdint>
#include <cstring>

// Message tags as 4-byte ASCII codes
struct Tag {
    char v[4];
    Tag(const char* s) { std::memcpy(v, s, 4); }
};

inline bool tagEq(const char* a, const char* b) {
    return std::memcmp(a, b, 4) == 0;
}

// Protocol constants
static constexpr int DEFAULT_CHUNK = 1460;        // payload per DATA
static constexpr int MAX_NACK_IDS_PER_MSG = 320;  // tune to keep UDP size < MTU
static constexpr int INFO_REPEATS = 6;            // repeat INFO for reliability
static constexpr int CTRL_REPEATS = 24;           // repeat P1DN/DONE/FIN for robustness

// Wire messages (packed)
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

struct DataMsg {
    char tag[4];           // "DATA"
    uint32_t seq;          // chunk id [0..chunk_count-1]
    uint16_t payload;      // bytes valid in data[]
    // Followed by payload bytes
};

struct CtrlMsg {
    char tag[4];           // "P1DN" / "DONE" / "FIN " / "FACK"
    uint64_t session_id;
};

struct NackMsgHdr {
    char tag[4];           // "NACK"
    uint16_t count;        // how many seq ids follow
    // Followed by count x uint32_t seq ids
};
#pragma pack(pop)
