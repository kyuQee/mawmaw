#pragma once
#include "core/window_registry.hpp"
#include <string>

namespace mawmaw::executor {

class IScript {
public:
    virtual ~IScript() = default;

    virtual std::string      id()      const = 0; // unique, used as handler map key
    virtual std::string      runtime() const = 0; // "python", "wasm", "native", ...
    virtual core::ScriptMode mode()    const = 0;

    // Zero-copy path — must return before ring wraps
    virtual core::ScriptOutput invoke_zero_copy(const core::ZeroCopyInput&) { return {}; }

    // Snapshot path — owns data, may take its time
    virtual core::ScriptOutput invoke_snapshot(const core::SnapshotInput&)  { return {}; }
};

} // namespace mawmaw::executor
