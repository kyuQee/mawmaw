// stream_router.hpp
#pragma once
#include "core/window_registry.hpp"
#include "core/telemetry.hpp"
#include "publisher/publisher.hpp"   // for Publisher reference
#include <iostream>
#include <atomic>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <condition_variable>
#include <mutex>
#include <optional>

namespace mawmaw::core {

struct Subscription {
    std::string trigger_stream;
    std::unordered_map<std::string, WindowSpec> windows;
};

using DispatchFn = std::function<ScriptOutput(const Event&)>;
using OutputFn   = std::function<void(ScriptOutput&&)>;

class StreamRouter {
public:
    struct HandlerEntry {
        Subscription sub;
        DispatchFn   dispatch;
        size_t       queue_index;  // which script queue this handler uses
    };

    StreamRouter(WindowRegistry& registry, OutputFn on_output)
        : registry_(registry)
        , on_output_(on_output) {}

    void set_telemetry(Telemetry* tel) {
        telemetry_.store(tel, std::memory_order_release);
    }

    // Set the routing table: stream → list of target IDs (script or publisher)
    void set_routes(const std::unordered_map<std::string, std::vector<std::string>>& routes) {
        route_table_ = routes;
    }

    // Set the publisher so we can forward events to endpoints
    void set_publisher(publisher::Publisher* pub) {
        publisher_ = pub;
    }

    // Create a new per-script event queue. Returns its index.
    size_t register_script_queue(size_t ring_capacity = 1024) {
        std::lock_guard<std::mutex> lock(queues_mutex_);
        size_t idx = queues_.size();
        queues_.emplace_back(std::make_unique<ScriptQueue>(ring_capacity));
        read_positions_.emplace_back(std::make_unique<std::atomic<uint64_t>>(0));
        return idx;
    }

    // Register a handler with a specific queue index.
    void register_handler(const std::string& script_id,
                          Subscription sub,
                          DispatchFn dispatch,
                          size_t queue_index) {
        handlers_[script_id] = HandlerEntry{std::move(sub), std::move(dispatch), queue_index};
    }

    // Hot Ingestion Path: use route table for explicit delivery.
    void route(Event ev) {
        Telemetry* tel = telemetry_.load(std::memory_order_acquire);
        if (tel) {
            tel->record_event(ev.stream_id);
        }

        // 1. Commit the event to the global window registry
        registry_.push(ev);

        // 2. Lookup the stream in the route table
        auto it = route_table_.find(ev.stream_id);
        if (it == route_table_.end()) {
            // No route defined – event is dropped (no processing, no publishing)
            return;
        }

        // 3. For each target, decide if it's a script or a publisher endpoint
        for (const auto& target_id : it->second) {
            // Check if target is a script handler
            auto handler_it = handlers_.find(target_id);
            if (handler_it != handlers_.end()) {
                // Enqueue to that script's queue
                size_t qidx = handler_it->second.queue_index;
                if (qidx < queues_.size()) {
                    auto& q = *queues_[qidx];
                    q.ring.write(ev);   // lock-free write
                    {
                        std::lock_guard<std::mutex> lock(q.mtx);
                        q.cv.notify_one();
                    }
                }
                continue;
            }

            // Otherwise, try to publish to a publisher endpoint
            if (publisher_ && publisher_->has_endpoint(target_id)) {
                publisher_->publish_to(ev, target_id);
            } else {
                // Unknown target – log or ignore
                std::cerr << "[StreamRouter] Unknown route target: " << target_id
                          << " for stream " << ev.stream_id << "\n";
            }
        }
    }

    // Block until an event arrives for the given queue, or until stopped.
    std::optional<Event> wait_for_trigger(size_t queue_idx) {
        if (queue_idx >= queues_.size()) return std::nullopt;
        auto& q = *queues_[queue_idx];
        std::unique_lock<std::mutex> lock(q.mtx);
        q.cv.wait(lock, [&] {
            uint64_t head = q.ring.head();
            uint64_t read_pos = read_positions_[queue_idx]->load(std::memory_order_acquire);
            return q.stopped.load(std::memory_order_acquire) || head > read_pos;
        });
        if (q.stopped.load(std::memory_order_acquire)) {
            return std::nullopt;
        }
        uint64_t read_pos = read_positions_[queue_idx]->load(std::memory_order_acquire);
        uint64_t head = q.ring.head();
        if (head <= read_pos) {
            return std::nullopt;  // spurious wake
        }
        const Event* ev = q.ring.read(read_pos);
        if (!ev) return std::nullopt;
        read_positions_[queue_idx]->store(read_pos + 1, std::memory_order_release);
        return *ev;
    }

    // Wake all waiting threads and prevent further blocking.
    void stop_all_queues() {
        for (auto& qptr : queues_) {
            qptr->stopped.store(true, std::memory_order_release);
            {
                std::lock_guard<std::mutex> lock(qptr->mtx);
                qptr->cv.notify_all();
            }
        }
    }

    const std::unordered_map<std::string, HandlerEntry>& get_handlers() const {
        return handlers_;
    }

    OutputFn get_output_callback() const {
        return on_output_;
    }

    ~StreamRouter() { std::cout << "StreamRouter destroyed\n"; }

private:
    struct ScriptQueue {
        RingBuffer<Event> ring;
        std::mutex mtx;
        std::condition_variable cv;
        std::atomic<bool> stopped{false};
        explicit ScriptQueue(size_t cap) : ring(cap) {}
    };

    WindowRegistry& registry_;
    OutputFn        on_output_;
    std::atomic<Telemetry*> telemetry_{nullptr};
    publisher::Publisher* publisher_ = nullptr;   // not owned

    std::unordered_map<std::string, HandlerEntry> handlers_;
    std::unordered_map<std::string, std::vector<std::string>> route_table_;   // stream → targets

    std::mutex queues_mutex_;
    std::vector<std::unique_ptr<ScriptQueue>> queues_;
    std::vector<std::unique_ptr<std::atomic<uint64_t>>> read_positions_;
};

} // namespace mawmaw::core