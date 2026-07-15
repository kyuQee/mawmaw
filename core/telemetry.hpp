#pragma once
#include "core/event.hpp"
#include "core/window_registry.hpp"   // for ScriptOutput
#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <vector>

// Forward declarations
namespace mawmaw::core { class StreamRouter; class WindowRegistry; }
namespace mawmaw::publisher { class Publisher; }

namespace mawmaw::core {

class Telemetry {
public:
    Telemetry(StreamRouter& router, WindowRegistry& registry, publisher::Publisher& publisher);
    ~Telemetry();

    void start(std::chrono::milliseconds interval = std::chrono::seconds(1));
    void stop();

    inline void record_event(const std::string& stream_id) {
        if (!enabled_) return;
        if (stream_id == "telemetry" || stream_id.rfind("telemetry.", 0) == 0) return;
        {
            std::shared_lock<std::shared_mutex> lock(event_counts_mutex_);
            auto it = event_counts_.find(stream_id);
            if (it != event_counts_.end()) {
                it->second.fetch_add(1, std::memory_order_relaxed);
                return;
            }
        }
        std::unique_lock<std::shared_mutex> lock(event_counts_mutex_);
        auto it = event_counts_.find(stream_id);
        if (it == event_counts_.end()) {
            it = event_counts_.emplace(std::piecewise_construct,
                                       std::forward_as_tuple(stream_id),
                                       std::forward_as_tuple(0)).first;
        }
        it->second.fetch_add(1, std::memory_order_relaxed);
    }

    inline void record_emitted(const std::string& stream_id) {
        if (!enabled_) return;
        if (stream_id == "telemetry" || stream_id.rfind("telemetry.", 0) == 0) return;
        {
            std::shared_lock<std::shared_mutex> lock(emitted_counts_mutex_);
            auto it = emitted_counts_.find(stream_id);
            if (it != emitted_counts_.end()) {
                it->second.fetch_add(1, std::memory_order_relaxed);
                return;
            }
        }
        std::unique_lock<std::shared_mutex> lock(emitted_counts_mutex_);
        auto it = emitted_counts_.find(stream_id);
        if (it == emitted_counts_.end()) {
            it = emitted_counts_.emplace(std::piecewise_construct,
                                         std::forward_as_tuple(stream_id),
                                         std::forward_as_tuple(0)).first;
        }
        it->second.fetch_add(1, std::memory_order_relaxed);
    }

    inline void record_drop() {
        if (!enabled_) return;
        drops_.fetch_add(1, std::memory_order_relaxed);
    }

    inline void record_script_invocation(const std::string& script_id, uint64_t duration_ns) {
        if (!enabled_) return;
        std::shared_lock<std::shared_mutex> lock(script_stats_mutex_);
        auto it = script_stats_.find(script_id);
        if (it == script_stats_.end()) return;
        auto& stats = it->second;
        stats.count.fetch_add(1, std::memory_order_relaxed);
        stats.total_latency_ns.fetch_add(duration_ns, std::memory_order_relaxed);
        uint64_t cur_min = stats.min_latency_ns.load(std::memory_order_relaxed);
        while (duration_ns < cur_min &&
               !stats.min_latency_ns.compare_exchange_weak(cur_min, duration_ns, std::memory_order_relaxed)) {}
        uint64_t cur_max = stats.max_latency_ns.load(std::memory_order_relaxed);
        while (duration_ns > cur_max &&
               !stats.max_latency_ns.compare_exchange_weak(cur_max, duration_ns, std::memory_order_relaxed)) {}
    }

    void register_script_stats(const std::string& script_id);
    void set_enabled(bool en) { enabled_ = en; }

private:
    void emit_metrics();
    bool pack_stream_metrics(uint64_t timestamp_ns, std::vector<std::vector<uint8_t>>& out_chunks);
    bool pack_script_metrics(uint64_t timestamp_ns, std::vector<std::vector<uint8_t>>& out_chunks);

    StreamRouter& router_;
    WindowRegistry& registry_;
    publisher::Publisher& publisher_;   // <-- added
    std::atomic<bool> enabled_{false};
    std::atomic<bool> running_{false};
    std::thread thread_;
    std::chrono::milliseconds interval_;

    std::unordered_map<std::string, std::atomic<uint64_t>> event_counts_;
    std::shared_mutex event_counts_mutex_;
    std::unordered_map<std::string, std::atomic<uint64_t>> emitted_counts_;
    std::shared_mutex emitted_counts_mutex_;
    std::atomic<uint64_t> drops_{0};

    struct ScriptStats {
        std::atomic<uint64_t> count{0};
        std::atomic<uint64_t> total_latency_ns{0};
        std::atomic<uint64_t> min_latency_ns{UINT64_MAX};
        std::atomic<uint64_t> max_latency_ns{0};
    };
    std::unordered_map<std::string, ScriptStats> script_stats_;
    std::shared_mutex script_stats_mutex_;
};

} // namespace mawmaw::core