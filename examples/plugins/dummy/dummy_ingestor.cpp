#include "ingestor/i_ingestor.hpp"

#include <atomic>
#include <chrono>
#include <cstring>
#include <pthread.h>
#include <thread>

class DummyIngestor final : public mawmaw::ingestor::IIngestor {
public:
    std::string name() const override { return "dummy"; }
    std::string version() const override { return "0.1.0"; }

    void start(mawmaw::ingestor::EmitFn emit) override {
        running_ = true;

        worker_ = std::thread([this, emit = std::move(emit)]() mutable {
            // Pin the producer thread to a dedicated CPU core.
            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(2, &cpuset);

            if (pthread_setaffinity_np(
                    pthread_self(),
                    sizeof(cpuset),
                    &cpuset) != 0)
            {
                // Affinity failed; continue anyway.
            }

            uint64_t seq = 0;

            while (running_.load(std::memory_order_relaxed)) {
                uint64_t current_timestamp =
                    static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now().time_since_epoch())
                            .count());

                mawmaw::core::Event ev;

                ev.timestamp_ns = current_timestamp;
                ev.sequence     = seq++;

                ev.set_stream("dummy_ticks");
                ev.set_schema("tick_v1");
                ev.set_payload(&current_timestamp, sizeof(current_timestamp));

                emit(std::move(ev));
            }
        });
    }

    void stop() override {
        running_.store(false, std::memory_order_relaxed);

        if (worker_.joinable())
            worker_.join();
    }

private:
    std::atomic<bool> running_{false};
    std::thread worker_;
};

extern "C" {

mawmaw::ingestor::IIngestor* mawmaw_create() {
    return new DummyIngestor();
}

void mawmaw_destroy(mawmaw::ingestor::IIngestor* p) {
    delete p;
}

const char* mawmaw_plugin_version() {
    return "0.1.0";
}

}