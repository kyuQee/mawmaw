#pragma once
#include "core/event.hpp"
#include "core/window_registry.hpp"
#include "core/telemetry.hpp"

#include <cstring>   // for std::strncmp
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>

namespace mawmaw::publisher {

class IEndpoint {
public:
    virtual ~IEndpoint()                          = default;
    virtual std::string name()              const = 0;
    virtual void publish(const core::Event& ev)   = 0;
};

class Publisher {
public:
    using ReinjectFn = std::function<void(core::Event)>;

    explicit Publisher(ReinjectFn reinject, core::Telemetry* telemetry = nullptr)
        : reinject_(std::move(reinject)), telemetry_(telemetry) {}

    void set_telemetry(core::Telemetry* telemetry) { telemetry_ = telemetry; }

    void add_endpoint(std::shared_ptr<IEndpoint> ep) {
        std::lock_guard lock(mu_);
        endpoints_[ep->name()] = std::move(ep);
    }

    void handle_output(core::ScriptOutput output) {
        for (auto& ev : output.emitted) {
            if (ev.lineage_depth >= max_lineage_depth_) continue;
            {
                std::lock_guard lock(mu_);
                for (auto& [name, ep] : endpoints_)
                    ep->publish(ev);
            }
            if (telemetry_) telemetry_->record_emitted(ev.stream_id);

            // ── Prevent re‑injection of telemetry events ──
            if (std::strncmp(ev.stream_id, "telemetry", 9) == 0 ||
                std::strncmp(ev.stream_id, "telemetry.", 10) == 0) {
                continue;
            }

            core::Event reinjected = ev;
            reinjected.lineage_depth++;
            reinject_(std::move(reinjected));
        }
    }

    void set_max_lineage_depth(uint32_t d) { max_lineage_depth_ = d; }

private:
    ReinjectFn  reinject_;
    core::Telemetry* telemetry_ = nullptr;
    std::mutex  mu_;
    std::unordered_map<std::string, std::shared_ptr<IEndpoint>> endpoints_;
    uint32_t    max_lineage_depth_ = 16;
};

} // namespace mawmaw::publisher