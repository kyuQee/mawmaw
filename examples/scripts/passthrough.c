#define STREAM_ID_MAX 32
#define SCHEMA_ID_MAX 16
#define PAYLOAD_MAX   256

typedef unsigned char   u8;
typedef unsigned short u16;
typedef unsigned int    u32;
typedef unsigned long long u64;

static u64 r64(const u8* p) {
    return (u64)p[0]|((u64)p[1]<<8)|((u64)p[2]<<16)|((u64)p[3]<<24)|
           ((u64)p[4]<<32)|((u64)p[5]<<40)|((u64)p[6]<<48)|((u64)p[7]<<56);
}
static u32 r32(const u8* p) { return p[0]|(p[1]<<8)|(p[2]<<16)|(p[3]<<24); }
static u16 r16(const u8* p) { return p[0]|(p[1]<<8); }
static void w64(u8* p, u64 v) { for(int i=0;i<8;i++) { p[i]=v&0xFF; v>>=8; } }
static void w32(u8* p, u32 v) { for(int i=0;i<4;i++) { p[i]=v&0xFF; v>>=8; } }
static void w16(u8* p, u16 v) { p[0]=v&0xFF; p[1]=(v>>8)&0xFF; }
static void mc(u8* d, const u8* s, u32 n) { for(u32 i=0;i<n;i++) d[i]=s[i]; }

/* Host runtime interfaces */
__attribute__((import_module("mawmaw"), import_name("time_ns")))
u64 host_time_ns(void);

__attribute__((import_module("mawmaw"), import_name("log")))
void host_log(const char* str);

/* Logging helpers – kept but not used */
static char log_buf[256];
static u32 log_idx = 0;

static void reset_log(void) {
    log_idx = 0;
    log_buf[0] = '\0';
}

static void append_s(const char* s) {
    while (*s && log_idx < 254) {
        log_buf[log_idx++] = *s++;
    }
    log_buf[log_idx] = '\0';
}

static void append_u(u64 num) {
    char temp[24];
    int i = 0;
    if (num == 0) {
        append_s("0");
        return;
    }
    while (num > 0) {
        temp[i++] = (num % 10) + '0';
        num /= 10;
    }
    while (i > 0 && log_idx < 254) {
        log_buf[log_idx++] = temp[--i];
    }
    log_buf[log_idx] = '\0';
}

static void append_stream_id(const u8* sid, u32 max_len) {
    u32 len = 0;
    while (len < max_len && sid[len] != '\0' && log_idx < 254) {
        log_buf[log_idx++] = (char)sid[len++];
    }
    log_buf[log_idx] = '\0';
}

static void append_frac3(u64 num) {
    if (log_idx < 251) {
        log_buf[log_idx++] = (num / 100) % 10 + '0';
        log_buf[log_idx++] = (num / 10) % 10 + '0';
        log_buf[log_idx++] = num % 10 + '0';
        log_buf[log_idx] = '\0';
    }
}

/* Core logic with bounds checking – no logging */
__attribute__((export_name("on_trigger")))
int on_trigger(int in_ptr, int in_len, int out_ptr, int out_max) {
    const u8* in = (const u8*)in_ptr;
    u8* out = (u8*)out_ptr;

    // Read fixed header
    u64 ts       = r64(in); in += 8;
    u64 seq      = r64(in); in += 8;
    u32 lineage  = r32(in); in += 4;
    u16 plen     = r16(in); in += 2;

    // Read stream and schema IDs (fixed size)
    const u8* stream_id = in;
    in += STREAM_ID_MAX;
    const u8* schema_id = in;
    in += SCHEMA_ID_MAX;
    const u8* payload = in;

    // Sanity check: payload length must not exceed maximum
    if (plen > PAYLOAD_MAX) {
        // reset_log();
        // append_s("ERROR: plen too large: ");
        // append_u(plen);
        // host_log(log_buf);
        return -1;
    }

    // Compute required output size
    const u32 header_size = 4 + 8 + 8 + 4 + 2;  // magic, ts, seq, lineage, plen
    const u32 total_needed = header_size + STREAM_ID_MAX + SCHEMA_ID_MAX + plen;
    if ((u32)out_max < total_needed) {
        // reset_log();
        // append_s("ERROR: output buffer too small (need ");
        // append_u(total_needed);
        // append_s(", got ");
        // append_u(out_max);
        // append_s(")");
        // host_log(log_buf);
        return -1;
    }

    // Extract timestamp from payload (first 8 bytes)
    u64 payload_ts = (plen >= 8) ? r64(payload) : 0;
    u64 current_ns = host_time_ns();
    long long latency_ns = (long long)current_ns - (long long)payload_ts;

    // All logging removed
    // reset_log();
    // append_s("stream=");
    // append_stream_id(stream_id, STREAM_ID_MAX);
    // append_s(" seq=");
    // append_u(seq);
    // host_log(log_buf);

    // reset_log();
    // append_s("Payload Time (ns): ");
    // append_u(payload_ts);
    // host_log(log_buf);

    // reset_log();
    // append_s("Current Time (ns): ");
    // append_u(current_ns);
    // host_log(log_buf);

    // reset_log();
    // append_s("Processing Latency: ");
    // if (latency_ns < 0) {
    //     append_s("-");
    //     latency_ns = -latency_ns;
    // }
    // append_u(latency_ns / 1000000);
    // append_s(".");
    // append_frac3((u64)((latency_ns % 1000000) / 1000));
    // append_s(" ms\n---");
    // host_log(log_buf);

    // Pack output buffer
    u8* p = out;
    w32(p, 1); p += 4;                     // magic
    w64(p, ts); p += 8;                    // timestamp
    w64(p, seq); p += 8;                   // sequence
    w32(p, lineage); p += 4;               // lineage
    w16(p, plen); p += 2;                  // payload length

    // Write stream ID "wasm_processed" (14 chars + null) padded to 32 bytes
    const char* stream_name = "wasm_processed";
    u32 name_len = 14;  // excluding null
    mc(p, (const u8*)stream_name, name_len);
    // Zero out the remaining bytes
    for (u32 i = name_len; i < STREAM_ID_MAX; i++) {
        p[i] = 0;
    }
    p += STREAM_ID_MAX;

    // Write schema ID "wasm_v1" (7 chars + null) padded to 16 bytes
    const char* schema_name = "wasm_v1";
    name_len = 7;
    mc(p, (const u8*)schema_name, name_len);
    for (u32 i = name_len; i < SCHEMA_ID_MAX; i++) {
        p[i] = 0;
    }
    p += SCHEMA_ID_MAX;

    // Copy payload
    mc(p, payload, plen);
    p += plen;

    return (int)(p - out);
}