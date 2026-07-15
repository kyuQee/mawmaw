#pragma once
#include "executor/i_script.hpp"
#include "executor/wasm/wasm_engine.hpp"

#include "wasm3.h"

#include <cstring>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>
#include <chrono>
#include <iostream>

extern "C"
{
    m3ApiRawFunction(host_time_ns)
    {
        m3ApiReturnType(uint64_t);

        auto now =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();

        m3ApiReturn((uint64_t)now);
    }

    m3ApiRawFunction(host_log)
    {
        m3ApiGetArgMem(const char *, ptr);

        std::cout << "[wasm] " << ptr << '\n';

        m3ApiSuccess();
    }
}

namespace mawmaw::executor::wasm
{
    // High-speed, raw-pointer based serialization mapping directly onto target memory blocks
    static inline void w16_ptr(uint8_t*& p, uint16_t v) {
        p[0] = v & 0xFF; p[1] = v >> 8; p += 2;
    }
    static inline void w32_ptr(uint8_t*& p, uint32_t v) {
        p[0] = v & 0xFF; p[1] = (v >> 8) & 0xFF;
        p[2] = (v >> 16) & 0xFF; p[3] = (v >> 24) & 0xFF; p += 4;
    }
    static inline void w64_ptr(uint8_t*& p, uint64_t v) {
        for (int i = 0; i < 8; i++) { p[i] = v & 0xFF; v >>= 8; } p += 8;
    }
    static inline void wev_ptr(uint8_t*& p, const core::Event &e) {
        w64_ptr(p, e.timestamp_ns);
        w64_ptr(p, e.sequence);
        w32_ptr(p, e.lineage_depth);
        w16_ptr(p, e.payload_size);
        std::memcpy(p, e.stream_id, core::STREAM_ID_MAX); p += core::STREAM_ID_MAX;
        std::memcpy(p, e.schema_id, core::SCHEMA_ID_MAX); p += core::SCHEMA_ID_MAX;
        std::memcpy(p, e.payload, e.payload_size); p += e.payload_size;
    }

    static uint16_t r16(const uint8_t *p) { return p[0] | (p[1] << 8); }
    static uint32_t r32(const uint8_t *p) { return p[0] | (p[1] << 8) | (p[2] << 16) | (p[3] << 24); }
    static uint64_t r64(const uint8_t *p)
    {
        uint64_t v = 0;
        for (int i = 7; i >= 0; i--)
            v = (v << 8) | p[i];
        return v;
    }
    static core::Event rev(const uint8_t *p)
    {
        core::Event e;
        e.timestamp_ns = r64(p);
        p += 8;
        e.sequence = r64(p);
        p += 8;
        e.lineage_depth = r32(p);
        p += 4;
        e.payload_size = r16(p);
        p += 2;
        std::memcpy(e.stream_id, p, core::STREAM_ID_MAX);
        p += core::STREAM_ID_MAX;
        std::memcpy(e.schema_id, p, core::SCHEMA_ID_MAX);
        p += core::STREAM_ID_MAX;
        std::memcpy(e.payload, p, e.payload_size);
        return e;
    }

    class WasmScript final : public IScript
    {
    public:
        static constexpr uint32_t STACK_SIZE = 64 * 1024;
        static constexpr size_t IO_BUF_SIZE = 64 * 1024;

        WasmScript(const std::string &id, const std::string &path, WasmEngine &engine)
            : id_(id), path_(path)
        {
            std::ifstream f(path, std::ios::binary | std::ios::ate);
            if (!f)
                throw std::runtime_error("WasmScript: cannot open " + path);
            auto sz = f.tellg();
            f.seekg(0);
            bytes_.resize(static_cast<size_t>(sz));
            f.read(reinterpret_cast<char *>(bytes_.data()), sz);

            runtime_ = m3_NewRuntime(engine.env(), STACK_SIZE, nullptr);
            if (!runtime_)
                throw std::runtime_error("WasmScript: m3_NewRuntime failed");

            M3Result r = m3_ParseModule(engine.env(), &module_,
                                        bytes_.data(), static_cast<uint32_t>(bytes_.size()));
            if (r)
                throw std::runtime_error("WasmScript: parse: " + std::string(r));

            r = m3_LoadModule(runtime_, module_);
            if (r)
                throw std::runtime_error("WasmScript: load: " + std::string(r));

            r = m3_LinkRawFunction(module_, "mawmaw", "time_ns", "I()", host_time_ns);
            if (r && std::string(r).find("lookup failed") == std::string::npos)
                throw std::runtime_error("link time_ns failed: " + std::string(r));

            r = m3_LinkRawFunction(module_, "mawmaw", "log", "v(i)", host_log);
            if (r && std::string(r).find("lookup failed") == std::string::npos)
                throw std::runtime_error("link log failed: " + std::string(r));

            r = m3_FindFunction(&fn_, runtime_, "on_trigger");
            if (r)
                throw std::runtime_error("WasmScript: " + path + " must export on_trigger: " + r);
        }

        ~WasmScript()
        {
            if (runtime_)
                m3_FreeRuntime(runtime_);
        }

        std::string id() const override { return id_; }
        std::string runtime() const override { return "wasm"; }
        core::ScriptMode mode() const override { return core::ScriptMode::ZeroCopy; }

        core::ScriptOutput invoke_zero_copy(const core::ZeroCopyInput &input) override
        {
            uint32_t mem_size = 0;
            uint8_t *mem = m3_GetMemory(runtime_, &mem_size, 0);
            if (!mem || mem_size < IO_BUF_SIZE * 2)
                return {};

            // Pure Zero-Copy Execution: Stream bytes directly onto the raw WASM memory address space
            uint8_t* write_ptr = mem;

            // ---------- 1. Serialize trigger event ----------
            wev_ptr(write_ptr, *input.trigger_event);

            // ---------- 2. Serialize windows ----------
            w32_ptr(write_ptr, static_cast<uint32_t>(input.windows.size()));
            for (const auto &[sid, view] : input.windows) {
                uint8_t sid_buf[core::STREAM_ID_MAX] = {};
                std::strncpy(reinterpret_cast<char *>(sid_buf), sid.c_str(), core::STREAM_ID_MAX - 1);
                
                std::memcpy(write_ptr, sid_buf, core::STREAM_ID_MAX);
                write_ptr += core::STREAM_ID_MAX;

                w32_ptr(write_ptr, static_cast<uint32_t>(view.size()));

                const auto *ring = view.ring;
                for (uint64_t seq = view.begin_seq; seq < view.end_seq; ++seq) {
                    const core::Event *ev = ring->read(seq);
                    if (!ev) continue;
                    wev_ptr(write_ptr, *ev);
                }
            }

            // ---------- 3. Fast Boundary Safety Verification ----------
            size_t written_in_bytes = write_ptr - mem;
            if (written_in_bytes > IO_BUF_SIZE)
                return {};

            uint32_t out_off = static_cast<uint32_t>(IO_BUF_SIZE);
            uint32_t in_len = static_cast<uint32_t>(written_in_bytes);

            // ---------- 4. Call WASM engine natively ----------
            M3Result r = m3_CallV(fn_, (uint32_t)0, in_len, out_off, (uint32_t)IO_BUF_SIZE);
            if (r)
                return {};

            uint32_t written = 0;
            m3_GetResultsV(fn_, &written);
            if (!written || written > IO_BUF_SIZE)
                return {};

            // ---------- 5. Parse output from response zone ----------
            const uint8_t *out = mem + out_off;
            uint32_t n = r32(out);
            out += 4;
            core::ScriptOutput result;
            result.emitted.reserve(n);
            for (uint32_t i = 0; i < n; ++i) {
                result.emitted.push_back(rev(out));
                out += 8 + 8 + 4 + 2 + core::STREAM_ID_MAX + core::SCHEMA_ID_MAX + result.emitted.back().payload_size;
            }
            return result;
        }

        core::ScriptOutput invoke_snapshot(const core::SnapshotInput &) override
        {
            return {};
        }

    private:
        std::string id_, path_;
        std::vector<uint8_t> bytes_;
        IM3Runtime runtime_ = nullptr;
        IM3Module module_ = nullptr;
        IM3Function fn_ = nullptr;
    };
} // namespace mawmaw::executor::wasm