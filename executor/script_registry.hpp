#pragma once
#include "core/stream_router.hpp"
#include "core/window_registry.hpp"
#include "executor/i_script.hpp"
#include <memory>
#include <iostream>

namespace mawmaw::executor
{

    // Bridges IScript ↔ StreamRouter.
    // Builds a DispatchFn lambda per script and registers it with the router.
    // The router never sees IScript — it only sees the callable.
    class ScriptRegistry
    {
    public:
        ScriptRegistry(core::StreamRouter &router, core::WindowRegistry &registry)
            : router_(router), registry_(registry) {}

        void register_script(std::shared_ptr<IScript> script, core::Subscription sub)
        {
            auto dispatch = [script, &registry = registry_, sub](
                                const core::Event &trigger) -> core::ScriptOutput
            {
                if (script->mode() == core::ScriptMode::ZeroCopy)
                {
                    // std::cout << "[DEBUG] ZeroCopy branch for " << script->id() << "\n";
                    return script->invoke_zero_copy(registry.make_zero_copy_input(trigger, sub.windows));
                }
                else
                {
                    // std::cout << "[DEBUG] Snapshot branch for " << script->id() << "\n";
                    return script->invoke_snapshot(registry.make_snapshot_input(trigger, sub.windows));
                }


            };
            router_.register_handler(script->id(), std::move(sub), std::move(dispatch));
        }

    private:
        core::StreamRouter &router_;
        core::WindowRegistry &registry_;
    };

} // namespace mawmaw::executor
