# MAWMAW Ingestor Plugin Guide

## 1. Overview

MAWMAW is a high‑performance telemetry and event processing engine written in C++20. Its core is **never recompiled** — instead, all data sources are loaded at runtime as **shared object (`.so`) plugins** via `dlopen`.

An **Ingestor Plugin** is a `.so` file that:
- Runs on its own thread
- Produces `Event` objects
- Knows **nothing** about routing, scripts, or publishing — it only emits events
- Receives an `EmitFn` callback from the core and calls it whenever it has data

---

## 2. The Event Structure

Everything that flows through MAWMAW is a fixed‑size, zero‑heap `Event` struct. It fits in ~328 bytes (~5 cache lines).

```cpp
namespace mawmaw::core {

static constexpr size_t STREAM_ID_MAX = 32;   // 31 chars + null
static constexpr size_t SCHEMA_ID_MAX = 16;   // 15 chars + null
static constexpr size_t PAYLOAD_MAX = 256;    // raw bytes

struct Event {
    uint64_t timestamp_ns = 0;      // wall clock at ingest (ns since epoch)
    uint64_t sequence = 0;          // monotonic counter, assigned by ingestor
    uint32_t lineage_depth = 0;     // cycle guard, incremented on re‑injection
    uint16_t payload_size = 0;      // actual used bytes in payload[]
    char stream_id[STREAM_ID_MAX] = {};
    char schema_id[SCHEMA_ID_MAX] = {};
    uint8_t payload[PAYLOAD_MAX] = {};
};
```

### Helper Methods

The struct provides safe helpers for setting fields:

```cpp
void set_stream(const char* s);      // truncates to 31 chars
void set_schema(const char* s);      // truncates to 15 chars
bool set_payload(const void* data, size_t len);  // returns false if len > 256
bool stream_is(const char* s) const;
```

---

## 3. Plugin Entry Point (The Contract)

MAWMAW loads your plugin and expects a **C‑compatible function** that starts the ingestion loop:

```cpp
extern "C" void ingestor_main(EmitFn emit);
```

Where `EmitFn` is a callback provided by the core. Your plugin calls `emit(event)` for every event it produces.

### Minimal Plugin Skeleton

```cpp
#include <mawmaw/core/event.hpp>   // ⚠️ See Section 4 for include path setup
#include <cstring>
#include <thread>
#include <atomic>

using EmitFn = void(*)(const mawmaw::core::Event&);

extern "C" void ingestor_main(EmitFn emit) {
    // Your ingestion loop — runs until the process stops.
    // For example, read from a socket, a file, or a hardware device.
    
    mawmaw::core::Event ev;
    ev.timestamp_ns = /* current time */;
    ev.sequence = /* your monotonic counter */;
    ev.set_stream("my_stream");          // routing key
    ev.set_schema("my_schema_v1");       // payload hint (optional)
    ev.set_payload(raw_data, len);       // your data
    
    emit(ev);  // push it into the pipeline
}
```

> **Important:**  
> - The plugin runs on its own thread; you can loop forever or use blocking I/O.  
> - The core does **not** manage your thread — you are responsible for your own lifecycle.  
> - MAWMAW is encoding‑agnostic: the payload can be MessagePack, FlatBuffers, JSON, raw binary, etc..

---

## 4. Compiling Your Plugin

Your plugin must be compiled as a **shared library** (`.so` on Linux).

### 4.1. Getting the Headers

MAWMAW's headers **are not installed separately** — they live in the source repository. You have two options:

**Option A: Clone the entire repository (recommended)**
```bash
git clone --recurse-submodules https://github.com/kyuQee/mawmaw.git
cd mawmaw
```

**Option B: Download only the `core/` directory**
```bash
mkdir -p mawmaw/core
curl -o mawmaw/core/event.hpp \
    https://raw.githubusercontent.com/kyuQee/mawmaw/master/core/event.hpp
# Also download any other headers you need (ring_buffer.hpp, etc.)
```

### 4.2. Include Path

The key is to point `-I` to the **parent directory** of the `mawmaw/` folder — not to `mawmaw/core/` directly. This is because your code uses `#include <mawmaw/core/event.hpp>`.

If you cloned the repo into `/home/user/mawmaw/`, the correct include path is:
```
-I/home/user/mawmaw
```

### 4.3. Manual Compilation

```bash
g++ -shared -fPIC -std=c++20 \
    -I/path/to/mawmaw \          # ⬅️ points to the repo root
    my_ingestor.cpp \
    -o my_ingestor.so \
    -pthread
```

### 4.4. Example CMake Snippet

```cmake
# Assuming MAWMAW_SOURCE_DIR points to the cloned repo
add_library(my_ingestor SHARED my_ingestor.cpp)
target_include_directories(my_ingestor PRIVATE ${MAWMAW_SOURCE_DIR})
target_link_libraries(my_ingestor PRIVATE dl)
set_target_properties(my_ingestor PROPERTIES PREFIX "" SUFFIX ".so")
```

---

## 5. Configuration

Once compiled, you reference your plugin in the MAWMAW config file under the `[ingestor]` section:

```ini
[ingestor]
id = my_feed
plugin = ./my_ingestor.so
as = custom_stream_name   # optional: rename the stream
```

- `id` – unique identifier for this ingestor instance  
- `plugin` – path to the `.so` file  
- `as` – (optional) renames the stream that this ingestor produces  

Without `as`, the stream name is the hardcoded value you set via `ev.set_stream()`.

---

## 6. Best Practices

### 6.1. Assign Monotonic Sequences
Each ingestor should assign a **monotonic `sequence`** to every event. Together with `stream_id`, this uniquely identifies any event and helps detect gaps.

### 6.2. Set Timestamps at Ingest
Populate `timestamp_ns` with the wall‑clock time when the data is ingested. Time‑based windows rely on this field.

### 6.3. Keep Payloads Small
The payload is limited to **256 bytes**. For larger data, you must chunk it into multiple events.

### 6.4. Avoid Heap Allocations
The hot path should be allocation‑free. Pre‑allocate buffers and reuse them.

### 6.5. Handle Backpressure
MAWMAW’s ring buffers have finite capacity. If your ingestor produces events faster than they can be consumed, events will be dropped. Consider implementing flow control or rate limiting.

---

## 7. Debugging Tips

- Use `dlerror()` to check if your plugin loads correctly.  
- Log from your plugin to `stderr` — MAWMAW will forward those messages.  
- Test with the `null` publisher or `stdout` publisher to verify event emission.  
- The `telemetry` publisher shows live stream statistics.

---

## 8. Complete Example

A simple ingestor that reads lines from a file and emits them as events:

```cpp
#include <mawmaw/core/event.hpp>
#include <fstream>
#include <string>
#include <thread>

using EmitFn = void(*)(const mawmaw::core::Event&);

extern "C" void ingestor_main(EmitFn emit) {
    std::ifstream file("/var/log/my_data.log");
    std::string line;
    uint64_t seq = 0;
    
    while (std::getline(file, line)) {
        mawmaw::core::Event ev;
        ev.timestamp_ns = std::chrono::system_clock::now().time_since_epoch().count();
        ev.sequence = seq++;
        ev.set_stream("raw_logs");
        ev.set_schema("text_line");
        ev.set_payload(line.data(), std::min(line.size(), size_t(256)));
        emit(ev);
        
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}
```

---

## 9. Further Reading

- Full documentation: [`docs/docs.md`](https://github.com/kyuQee/mawmaw/blob/master/docs/docs.md)  
- Configuration guide: [`config/config_guide.md`](https://github.com/kyuQee/mawmaw/blob/master/config/config_guide.md)  
- Core headers: `core/event.hpp`, `core/ring_buffer.hpp`, `core/window_registry.hpp`

