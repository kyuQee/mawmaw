#pragma once
#include "core/event.hpp"
#include <functional>
#include <string>

namespace mawmaw::ingestor {

// Callback type the plugin calls to push events into the pipeline
using EmitFn = std::function<void(core::Event)>;

// Every ingestor plugin implements this interface.
class IIngestor {
public:
    virtual ~IIngestor() = default;
    virtual std::string name()    const = 0;
    virtual std::string version() const = 0;
    virtual void start(EmitFn emit) = 0;
    virtual void stop() = 0;
};

} // namespace mawmaw::ingestor

// C ABI — every .so plugin exports these three symbols
extern "C" {
    mawmaw::ingestor::IIngestor* mawmaw_create();
    void                         mawmaw_destroy(mawmaw::ingestor::IIngestor*);
    const char* mawmaw_plugin_version();
}
