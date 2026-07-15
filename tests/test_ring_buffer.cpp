#include "core/ring_buffer.hpp"
#include "core/event.hpp"
#include <cassert>
#include <cstring>
#include <iostream>

int main() {
    using namespace mawmaw::core;

    // Basic write / read
    RingBuffer<Event> ring(16);
    assert(ring.head() == 0);

    Event e;
    e.sequence = 42;
    e.set_stream("test");
    ring.write(e);

    assert(ring.head() == 1);
    const Event* got = ring.read(0);
    assert(got != nullptr);
    assert(got->sequence == 42);
    assert(got->stream_is("test"));

    // RingView
    for (int i = 1; i < 10; ++i) {
        Event ev;
        ev.sequence = i + 42;
        ev.set_stream("test");
        ring.write(ev);
    }

    RingView<Event> view;
    view.ring      = &ring;
    view.begin_seq = 0;
    view.end_seq   = ring.head();

    assert(view.size() == 10);
    assert(view[0] != nullptr);
    assert(view[0]->sequence == 42 + 9); // newest first

    // Payload round-trip
    Event pev;
    pev.set_stream("test");
    uint64_t val = 0xDEADBEEFCAFEBABE;
    assert(pev.set_payload(&val, sizeof(val)));
    assert(pev.payload_size == 8);
    uint64_t out = 0;
    std::memcpy(&out, pev.payload, 8);
    assert(out == val);

    // Payload overflow guard
    uint8_t big[257] = {};
    assert(!pev.set_payload(big, 257));

    std::cout << "ring_buffer tests passed\n";
    return 0;
}
