#pragma once
#include <cstdint>
#include <cstring>

namespace mawmaw::core {

static constexpr size_t STREAM_ID_MAX  = 32;   // 31 chars + null
static constexpr size_t SCHEMA_ID_MAX  = 16;   // 15 chars + null
static constexpr size_t PAYLOAD_MAX    = 256;

// Every byte flowing through MAWMAW is wrapped in this.
// Fully inline — zero heap allocation. Fits in ~5 cache lines. (~328 bytes)
struct Event {
    uint64_t timestamp_ns              = 0;  // Wall clock at ingest (ns since epoch)
    uint64_t sequence                  = 0;  // Monotonic counter, assigned by ingestor
    uint32_t lineage_depth             = 0;  // Cycle guard. Incremented on re-injection. Drops at max (default 16)
    uint16_t payload_size              = 0;  // Actual used bytes in payload[]
    char     stream_id[STREAM_ID_MAX]  = {}; // Logical stream key (e.g. "trades"). Max 31 chars + null
    char     schema_id[SCHEMA_ID_MAX]  = {}; // Payload shape hint (e.g. "tick_v1"). Max 15 chars + null
    uint8_t  payload[PAYLOAD_MAX]      = {}; // Raw payload bytes (msgpack, flatbuf, etc.). Chunked if >256

    // ── helpers ───────────────────────────────────────────────────────────────

    // Sets the stream ID, safely truncating to STREAM_ID_MAX.
    void set_stream(const char* s) {
        std::strncpy(stream_id, s, STREAM_ID_MAX - 1);
        stream_id[STREAM_ID_MAX - 1] = '\0';
    }

    // Sets the schema ID, safely truncating to SCHEMA_ID_MAX.
    void set_schema(const char* s) {
        std::strncpy(schema_id, s, SCHEMA_ID_MAX - 1);
        schema_id[SCHEMA_ID_MAX - 1] = '\0';
    }

    // Writes up to PAYLOAD_MAX bytes into the event payload. Returns false if too large.
    bool set_payload(const void* data, size_t len) {
        if (len > PAYLOAD_MAX) return false;
        std::memcpy(payload, data, len);
        payload_size = static_cast<uint16_t>(len);
        return true;
    }

    // Checks if the stream ID matches the given string.
    bool stream_is(const char* s) const {
        return std::strncmp(stream_id, s, STREAM_ID_MAX) == 0;
    }
};

} // namespace mawmaw::core