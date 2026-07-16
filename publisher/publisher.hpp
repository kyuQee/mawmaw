#pragma once
#include "core/event.hpp"
#include "core/window_registry.hpp"
#include "core/telemetry.hpp"

#include <cstring>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>

namespace mawmaw::publisher {

class IEndpoint {
public:
    virtual ~IEndpoint() = default;
    virtual std::string name() const = 0;
    virtual void publish(const core::Event& ev) = 0;
};

class Publisher {
public:
    // ReinjectFn is no longer used – we keep for compatibility but ignore it.
    using ReinjectFn = std::function<void(core::Event)>;

    explicit Publisher(ReinjectFn /*unused*/ = nullptr, core::Telemetry* telemetry = nullptr)
        : telemetry_(telemetry) {}

    void set_telemetry(core::Telemetry* telemetry) { telemetry_ = telemetry; }

    void add_endpoint(std::shared_ptr<IEndpoint> ep) {
        std::lock_guard lock(mu_);
        endpoints_[ep->name()] = std::move(ep);
    }

    bool has_endpoint(const std::string& id) const {
        std::lock_guard lock(mu_);
        return endpoints_.find(id) != endpoints_.end();
    }

    // Publish an event to a single named endpoint.
    void publish_to(const core::Event& ev, const std::string& endpoint_id) {
        std::lock_guard lock(mu_);
        auto it = endpoints_.find(endpoint_id);
        if (it != endpoints_.end()) {
            it->second->publish(ev);
            if (telemetry_) telemetry_->record_emitted(ev.stream_id);
        }
    }

    // Broadcast an event to all endpoints (use sparingly, e.g., for telemetry).
    void broadcast(const core::Event& ev) {
        std::lock_guard lock(mu_);
        for (auto& [name, ep] : endpoints_) {
            ep->publish(ev);
        }
        if (telemetry_) telemetry_->record_emitted(ev.stream_id);
    }

    // Legacy: handle script output. We no longer re‑inject; the router does it.
    // This method is kept only for backward compatibility and does nothing.
    void handle_output(core::ScriptOutput /*output*/) {
        // Re‑injection and broadcasting are removed.
        // The router will handle routing of emitted events via its route table.
    }

    // Deprecated: lineage depth is now managed by the router's route table.
    void set_max_lineage_depth(uint32_t /*d*/) {}

private:
    core::Telemetry* telemetry_ = nullptr;
    mutable std::mutex mu_;
    std::unordered_map<std::string, std::shared_ptr<IEndpoint>> endpoints_;
};

} // namespace mawmaw::publisher