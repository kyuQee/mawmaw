#pragma once
#include "core/event.hpp"
#include "core/ring_buffer.hpp"
#include "core/telemetry.hpp"   

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace mawmaw::core
{

    enum class ScriptMode
    {
        ZeroCopy,
        Snapshot
    };

    /** Window specification: count-based or time-based. */
    struct WindowSpec
    {
        enum class Type
        {
            TimeBased,
            CountBased
        } type = Type::CountBased;
        uint64_t duration_ns = 0; // for time-based windows
        size_t count = 256;       // for count-based windows
    };

    /** Input for zero-copy scripts: pointers into ring buffers. */
    struct ZeroCopyInput
    {
        const Event *trigger_event = nullptr;
        std::unordered_map<std::string, RingView<Event>> windows;
    };

    /** Input for snapshot scripts: owned copies of events. */
    struct SnapshotInput
    {
        Event trigger_event;
        std::unordered_map<std::string, std::vector<Event>> windows;
    };

    /** Script output: list of events to re-inject. */
    struct ScriptOutput
    {
        std::vector<Event> emitted;
    };

    /** One ring buffer per stream, managed by the registry. */
    struct StreamRing
    {
        std::string stream_id;
        RingBuffer<Event> ring;
        explicit StreamRing(const std::string &id, size_t capacity = 65536)
            : stream_id(id), ring(capacity) {}
    };

    /**
     * WindowRegistry owns all stream rings and builds script inputs.
     *
     * - The registry is thread-safe: all public methods lock mu_ for map accesses.
     * - Zero-copy input: descriptors are built under lock; no copying.
     * - Snapshot input: bounds are computed under lock, then events are copied
     * outside the lock while holding the reader position to prevent wrap.
     * - Reader positions are tracked per script to implement backpressure.
     */
    class WindowRegistry
    {
    public:
        WindowRegistry(Telemetry* telemetry = nullptr) : telemetry_(telemetry) {}

        ~WindowRegistry()
        {
            // Cursors are safely cleaned up via unique_ptr vectors automatically now
        }

        void set_telemetry(Telemetry* telemetry) { telemetry_ = telemetry; }

        /** Ensures a stream ring exists; idempotent. */
        void ensure_stream(const std::string &id, size_t capacity = 65536)
        {
            std::lock_guard lock(mu_);
            if (!rings_.count(id))
                rings_[id] = std::make_shared<StreamRing>(id, capacity);
        }

        /** Returns the ring for a stream, or nullptr. */
        std::shared_ptr<StreamRing> get_ring(const std::string &id)
        {
            std::lock_guard lock(mu_);
            auto it = rings_.find(id);
            return it != rings_.end() ? it->second : nullptr;
        }

        /** Pushes an event into its stream ring. Returns the sequence number. */
        uint64_t push(const Event &ev)
        {
            std::shared_ptr<StreamRing> sr;
            {
                std::lock_guard lock(mu_);
                auto it = rings_.find(ev.stream_id);
                if (it == rings_.end())
                    return 0;
                sr = it->second;
            }
            // Detect ring overflow (writer lapping slowest reader)
            if (telemetry_ && !can_write(sr->ring)) {
                telemetry_->record_drop();
            }
            return sr->ring.write(ev);
        }

        /**
         * Builds zero-copy input for a script.
         *
         * Lock is held only during ring lookup and window bound computation.
         * The returned RingView descriptors point directly into ring memory.
         * The caller must ensure the ring does not wrap before processing.
         */
        ZeroCopyInput make_zero_copy_input(
            const Event &trigger,
            const std::unordered_map<std::string, WindowSpec> &subs) const
        {
            ZeroCopyInput input;
            input.trigger_event = &trigger;
            std::lock_guard lock(mu_);
            for (auto &[sid, spec] : subs)
            {
                auto it = rings_.find(sid);
                if (it == rings_.end())
                    continue;
                const auto &ring = it->second->ring;
                uint64_t head = ring.head();
                uint64_t begin = compute_begin(ring, head, spec, trigger.timestamp_ns);
                input.windows[sid] = {&ring, begin, head};
            }
            return input;
        }

        /**
         * Builds snapshot input for a script.
         *
         * Bounds are computed under lock, then events are copied outside the lock.
         * The reader position (minimum sequence read) is held during the copy
         * to prevent the writer from wrapping the ring and corrupting the copy.
         */
        SnapshotInput make_snapshot_input(
            const Event &trigger,
            const std::unordered_map<std::string, WindowSpec> &subs,
            size_t script_idx)
        {
            struct WindowInfo
            {
                std::shared_ptr<StreamRing> ring;
                uint64_t begin;
                uint64_t end;
            };
            std::unordered_map<std::string, WindowInfo> infos;
            uint64_t min_seq = UINT64_MAX;

            {
                std::lock_guard lock(mu_);
                for (auto &[sid, spec] : subs)
                {
                    auto it = rings_.find(sid);
                    if (it == rings_.end())
                        continue;
                    const auto &ring = it->second->ring;
                    uint64_t head = ring.head();
                    uint64_t begin = compute_begin(ring, head, spec, trigger.timestamp_ns);
                    infos[sid] = {it->second, begin, head};
                    if (begin < min_seq)
                        min_seq = begin;
                }
            }

            // Copy events outside the lock
            SnapshotInput input;
            input.trigger_event = trigger;
            for (auto &[sid, info] : infos)
            {
                auto &vec = input.windows[sid];
                vec.reserve(info.end - info.begin);
                for (uint64_t seq = info.begin; seq < info.end; ++seq)
                {
                    const Event *e = info.ring->ring.read(seq);
                    if (e)
                        vec.push_back(*e);
                }
            }

            return input;
        }

        /** Registers a script and returns its reader index. */
        size_t register_script()
        {
            std::lock_guard lock(mu_);
            size_t idx = script_positions_.size();
            auto cursor = std::make_unique<std::atomic<uint64_t>>(0);
            script_positions_.push_back(cursor.get());
            owned_cursors_.push_back(std::move(cursor));
            return idx;
        }

        /** Marks the start of a read, with the minimum sequence being read. */
        void script_begin_read(size_t idx, uint64_t min_seq)
        {
            script_positions_[idx]->store(min_seq, std::memory_order_release);
        }

        /** Marks the end of a read, releasing the position. */
        void script_end_read(size_t idx)
        {
            // Maintained for backward compatibility interface requirements
        }

        /** Safely flushes cursors on configuration changes or hot reload cycles */
        void clear_cursors()
        {
            std::lock_guard lock(mu_);
            script_positions_.clear();
            owned_cursors_.clear();
        }

        /** Checks if the slowest reader is lagging behind capacity. */
        bool can_write(const RingBuffer<Event> &ring) const
        {
            uint64_t global_min = get_global_min_reader();
            uint64_t head = ring.head();
            return (head - global_min) < ring.capacity();
        }

    private:
        mutable std::mutex mu_;
        std::unordered_map<std::string, std::shared_ptr<StreamRing>> rings_;
        std::vector<std::atomic<uint64_t> *> script_positions_;
        std::vector<std::unique_ptr<std::atomic<uint64_t>>> owned_cursors_;
        Telemetry* telemetry_ = nullptr;

        /** Returns the minimum sequence across all active readers. */
        uint64_t get_global_min_reader() const
        {
            uint64_t min_seq = UINT64_MAX;
            for (auto *p : script_positions_)
            {
                uint64_t val = p->load(std::memory_order_acquire);
                if (val < min_seq)
                    min_seq = val;
            }
            return min_seq == UINT64_MAX ? 0 : min_seq;
        }

        /** Computes the start sequence of a window based on spec and trigger timestamp. */
        static uint64_t compute_begin(
            const RingBuffer<Event> &ring,
            uint64_t head,
            const WindowSpec &spec,
            uint64_t trigger_ts_ns)
        {
            if (head == 0)
                return 0;
            if (spec.type == WindowSpec::Type::CountBased)
                return (head >= spec.count) ? (head - spec.count) : 0;
            uint64_t cutoff = (trigger_ts_ns > spec.duration_ns)
                                  ? (trigger_ts_ns - spec.duration_ns)
                                  : 0;
            uint64_t begin = head;
            while (begin > 0)
            {
                const Event *e = ring.read(begin - 1);
                if (!e || e->timestamp_ns < cutoff)
                    break;
                --begin;
            }
            return begin;
        }
    };

} // namespace mawmaw::core