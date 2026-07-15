#pragma once
#include <atomic>
#include <cassert>
#include <cstddef>
#include <optional>
#include <vector>

namespace mawmaw::core {

// Single-producer, multiple-consumer lock-free ring buffer.
//
// Writers claim slots via fetch_add on write_seq.
// Readers trail the writer with their own cursor — no coordination with writer.
// Ring MUST be power-of-two sized so index masking works.
//
// IMPORTANT CONTRACT: the ring must be large enough that the writer never
// laps the slowest reader. Violation = silent data corruption (by design —
// we prefer throughput over safety checks on the hot path).
// Size the ring generously and monitor lag via reader_lag().
template<typename T>
class RingBuffer {
public:
    explicit RingBuffer(size_t capacity)
        : capacity_(capacity)
        , mask_(capacity - 1)
        , slots_(capacity)
        , write_seq_(0)
    {
        assert((capacity & mask_) == 0 && "RingBuffer capacity must be power of two");
    }

    // Producer: write one item. Returns the sequence number assigned.
    // Not thread-safe with multiple producers — single producer only.
    uint64_t write(T item) {
        uint64_t seq = write_seq_.load(std::memory_order_relaxed);
        slots_[seq & mask_] = std::move(item);
        // Release so readers see the write before the sequence bump
        write_seq_.store(seq + 1, std::memory_order_release);
        return seq;
    }

    // Consumer: read at a specific sequence position.
    // Returns nullptr if seq hasn't been written yet.
    const T* read(uint64_t seq) const {
        uint64_t head = write_seq_.load(std::memory_order_acquire);
        if (seq >= head) return nullptr; // not written yet
        return &slots_[seq & mask_];
    }

    // Current write head — readers use this to know where the writer is.
    uint64_t head() const {
        return write_seq_.load(std::memory_order_acquire);
    }

    // How far behind is a reader at `reader_seq` from the current head.
    uint64_t reader_lag(uint64_t reader_seq) const {
        uint64_t h = head();
        return (h > reader_seq) ? (h - reader_seq) : 0;
    }

    size_t capacity() const { return capacity_; }

private:
    const size_t capacity_;
    const size_t mask_;
    std::vector<T> slots_;                          // fixed allocation, never resized
    alignas(64) std::atomic<uint64_t> write_seq_;  // cache-line isolated
};

// Lightweight view into a RingBuffer window.
// Handed to scripts on the zero-copy path — no allocation, no copy.
// ONLY valid while the ring hasn't wrapped past begin_seq.
template<typename T>
struct RingView {
    const RingBuffer<T>* ring    = nullptr;
    uint64_t             begin_seq = 0;   // oldest event in the window
    uint64_t             end_seq   = 0;   // one past the newest (== ring head at snapshot time)

    size_t size() const { return (end_seq > begin_seq) ? (end_seq - begin_seq) : 0; }
    bool   empty() const { return size() == 0; }

    // Iterate newest-first
    const T* operator[](size_t i) const {
        // i=0 is newest
        uint64_t seq = end_seq - 1 - i;
        if (seq < begin_seq) return nullptr;
        return ring->read(seq);
    }
};

} // namespace mawmaw::core
