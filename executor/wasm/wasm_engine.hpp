#pragma once
#include "wasm3.h"
#include <stdexcept>

namespace mawmaw::executor::wasm {

class WasmEngine {
public:
    WasmEngine() : env_(m3_NewEnvironment()) {
        if (!env_) throw std::runtime_error("WasmEngine: m3_NewEnvironment() failed");
    }
    ~WasmEngine() { if (env_) m3_FreeEnvironment(env_); }
    WasmEngine(const WasmEngine&)            = delete;
    WasmEngine& operator=(const WasmEngine&) = delete;
    WasmEngine(WasmEngine&&)                 = delete;
    WasmEngine& operator=(WasmEngine&&)      = delete;
    IM3Environment env() const { return env_; }
private:
    IM3Environment env_ = nullptr;
};

} // namespace mawmaw::executor::wasm
