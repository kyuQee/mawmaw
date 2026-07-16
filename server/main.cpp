#include "config/config.hpp"
#include "core/window_registry.hpp"
#include "core/stream_router.hpp"
#include "core/telemetry.hpp"
#include "executor/i_script.hpp"
#include "executor/python/python_engine.hpp"
#include "executor/python/python_script.hpp"
#include "executor/wasm/wasm_engine.hpp"
#include "executor/wasm/wasm_script.hpp"
#include "ingestor/plugin_loader.hpp"
#include "publisher/publisher.hpp"
#include "publisher/endpoints.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <unordered_map>
#include <condition_variable>
#include <mutex>

namespace fs = std::filesystem;

// ============================================================================
// Telemetry implementations – defined here because they need full types
// ============================================================================

namespace mawmaw::core
{

    Telemetry::Telemetry(StreamRouter &router, WindowRegistry &registry, publisher::Publisher &publisher)
        : router_(router), registry_(registry), publisher_(publisher) {}

    Telemetry::~Telemetry()
    {
        stop();
    }

    void Telemetry::start(std::chrono::milliseconds interval)
    {
        if (running_)
            return;
        enabled_ = true;
        running_ = true;
        interval_ = interval;
        thread_ = std::thread(&Telemetry::emit_metrics, this);
    }

    void Telemetry::stop()
    {
        if (!running_)
            return;
        running_ = false;
        if (thread_.joinable())
            thread_.join();
    }

    void Telemetry::register_script_stats(const std::string &script_id)
    {
        std::lock_guard<std::shared_mutex> lock(script_stats_mutex_);
        script_stats_.emplace(std::piecewise_construct,
                              std::forward_as_tuple(script_id),
                              std::forward_as_tuple());
    }

    void Telemetry::emit_metrics()
    {
        while (running_)
        {
            std::this_thread::sleep_for(interval_);
            if (!enabled_)
                continue;

            uint64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                               std::chrono::system_clock::now().time_since_epoch())
                               .count();
            if (now == 0)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();
            }

            std::vector<std::vector<uint8_t>> stream_chunks;
            if (pack_stream_metrics(now, stream_chunks))
            {
                for (auto &chunk : stream_chunks)
                {
                    Event ev;
                    ev.timestamp_ns = now;
                    ev.set_stream("telemetry");
                    ev.set_schema("streams_v1");
                    if (!ev.set_payload(chunk.data(), chunk.size()))
                    {
                        std::cerr << "[telemetry] BUG: streams_v1 chunk of "
                                  << chunk.size() << " bytes exceeds PAYLOAD_MAX ("
                                  << PAYLOAD_MAX << "), dropping\n";
                        continue;
                    }
                    router_.route(std::move(ev));
                }
            }

            std::vector<std::vector<uint8_t>> script_chunks;
            if (pack_script_metrics(now, script_chunks))
            {
                for (auto &chunk : script_chunks)
                {
                    Event ev;
                    ev.timestamp_ns = now;
                    ev.set_stream("telemetry");
                    ev.set_schema("scripts_v1");
                    if (!ev.set_payload(chunk.data(), chunk.size()))
                    {
                        std::cerr << "[telemetry] BUG: scripts_v1 chunk of "
                                  << chunk.size() << " bytes exceeds PAYLOAD_MAX ("
                                  << PAYLOAD_MAX << "), dropping\n";
                        continue;
                    }
                    router_.route(std::move(ev));
                }
            }
        }
    }

    bool Telemetry::pack_stream_metrics(uint64_t timestamp_ns, std::vector<std::vector<uint8_t>> &out_chunks)
    {
        out_chunks.clear();

        std::unordered_map<std::string, uint64_t> event_snap;
        {
            std::lock_guard<std::shared_mutex> lock(event_counts_mutex_);
            for (auto &kv : event_counts_)
            {
                uint64_t val = kv.second.exchange(0, std::memory_order_relaxed);
                if (val > 0)
                    event_snap[kv.first] = val;
            }
        }
        std::unordered_map<std::string, uint64_t> emitted_snap;
        {
            std::lock_guard<std::shared_mutex> lock(emitted_counts_mutex_);
            for (auto &kv : emitted_counts_)
            {
                uint64_t val = kv.second.exchange(0, std::memory_order_relaxed);
                if (val > 0)
                    emitted_snap[kv.first] = val;
            }
        }
        uint64_t drops = drops_.exchange(0, std::memory_order_relaxed);

        uint64_t total_events = 0;
        for (auto &kv : event_snap)
            total_events += kv.second;
        uint64_t total_emitted = 0;
        for (auto &kv : emitted_snap)
            total_emitted += kv.second;

        std::unordered_map<std::string, uint64_t> combined;
        for (auto &kv : event_snap)
            combined[kv.first] += kv.second;
        for (auto &kv : emitted_snap)
            combined[kv.first] += kv.second;

        if (combined.empty() && drops == 0)
            return false;

        constexpr size_t HEADER_SIZE = 8 + 8 + 8 + 8 + 1 + 1 + 2; // 36
        constexpr size_t ENTRY_SIZE = 32 + 8;                     // 40
        constexpr size_t MAX_ENTRIES_PER_CHUNK = (PAYLOAD_MAX - HEADER_SIZE) / ENTRY_SIZE;
        static_assert(MAX_ENTRIES_PER_CHUNK > 0, "PAYLOAD_MAX too small");

        std::vector<std::pair<std::string, uint64_t>> entries(combined.begin(), combined.end());
        size_t total_entries = entries.size();
        size_t chunk_count = total_entries == 0 ? 1 : (total_entries + MAX_ENTRIES_PER_CHUNK - 1) / MAX_ENTRIES_PER_CHUNK;

        auto w64 = [](std::vector<uint8_t> &out, uint64_t v)
        {
            for (int i = 0; i < 8; ++i)
            {
                out.push_back(v & 0xFF);
                v >>= 8;
            }
        };
        auto w16 = [](std::vector<uint8_t> &out, uint16_t v)
        {
            out.push_back(v & 0xFF);
            out.push_back((v >> 8) & 0xFF);
        };

        size_t idx = 0;
        for (size_t c = 0; c < chunk_count; ++c)
        {
            size_t n = std::min(MAX_ENTRIES_PER_CHUNK, total_entries - idx);
            std::vector<uint8_t> out;
            out.reserve(HEADER_SIZE + n * ENTRY_SIZE);

            w64(out, timestamp_ns);
            w64(out, total_events);
            w64(out, total_emitted);
            w64(out, drops);
            out.push_back((uint8_t)c);
            out.push_back((uint8_t)chunk_count);
            w16(out, (uint16_t)n);

            for (size_t i = 0; i < n; ++i)
            {
                const auto &[sid, count] = entries[idx + i];
                char buf[32] = {};
                std::strncpy(buf, sid.c_str(), 31);
                out.insert(out.end(), buf, buf + 32);
                w64(out, count);
            }
            idx += n;
            out_chunks.push_back(std::move(out));
        }
        return true;
    }

    bool Telemetry::pack_script_metrics(uint64_t timestamp_ns, std::vector<std::vector<uint8_t>> &out_chunks)
    {
        out_chunks.clear();

        if (timestamp_ns == 0)
        {
            timestamp_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                               std::chrono::system_clock::now().time_since_epoch())
                               .count();
        }

        struct SnapStats
        {
            uint64_t count;
            uint64_t total_latency_ns;
            uint64_t min_latency_ns;
            uint64_t max_latency_ns;
        };

        std::vector<std::pair<std::string, SnapStats>> snap;
        {
            std::lock_guard<std::shared_mutex> lock(script_stats_mutex_);
            for (auto &kv : script_stats_)
            {
                SnapStats st;
                st.count = kv.second.count.exchange(0, std::memory_order_relaxed);
                st.total_latency_ns = kv.second.total_latency_ns.exchange(0, std::memory_order_relaxed);
                st.min_latency_ns = kv.second.min_latency_ns.exchange(UINT64_MAX, std::memory_order_relaxed);
                st.max_latency_ns = kv.second.max_latency_ns.exchange(0, std::memory_order_relaxed);
                if (st.count > 0)
                {
                    snap.emplace_back(std::piecewise_construct,
                                      std::forward_as_tuple(kv.first),
                                      std::forward_as_tuple(std::move(st)));
                }
            }
        }
        if (snap.empty())
            return false;

        constexpr size_t HEADER_SIZE = 8 + 1 + 1 + 2;     // 12
        constexpr size_t ENTRY_SIZE = 32 + 8 + 8 + 8 + 8; // 64
        constexpr size_t MAX_ENTRIES_PER_CHUNK = (PAYLOAD_MAX - HEADER_SIZE) / ENTRY_SIZE;
        static_assert(MAX_ENTRIES_PER_CHUNK > 0, "PAYLOAD_MAX too small");

        size_t total_entries = snap.size();
        size_t chunk_count = (total_entries + MAX_ENTRIES_PER_CHUNK - 1) / MAX_ENTRIES_PER_CHUNK;

        auto w64 = [](std::vector<uint8_t> &out, uint64_t v)
        {
            for (int i = 0; i < 8; ++i)
            {
                out.push_back(v & 0xFF);
                v >>= 8;
            }
        };
        auto w16 = [](std::vector<uint8_t> &out, uint16_t v)
        {
            out.push_back(v & 0xFF);
            out.push_back((v >> 8) & 0xFF);
        };

        size_t idx = 0;
        for (size_t c = 0; c < chunk_count; ++c)
        {
            size_t n = std::min(MAX_ENTRIES_PER_CHUNK, total_entries - idx);
            std::vector<uint8_t> out;
            out.reserve(HEADER_SIZE + n * ENTRY_SIZE);

            w64(out, timestamp_ns);
            out.push_back((uint8_t)c);
            out.push_back((uint8_t)chunk_count);
            w16(out, (uint16_t)n);

            for (size_t i = 0; i < n; ++i)
            {
                const auto &[sid, st] = snap[idx + i];
                char buf[32] = {};
                std::strncpy(buf, sid.c_str(), 31);
                out.insert(out.end(), buf, buf + 32);

                uint64_t avg = (st.count > 0) ? (st.total_latency_ns / st.count) : 0;
                w64(out, st.count);
                w64(out, avg);
                w64(out, st.min_latency_ns == UINT64_MAX ? 0 : st.min_latency_ns);
                w64(out, st.max_latency_ns);
            }
            idx += n;
            out_chunks.push_back(std::move(out));
        }
        return true;
    }

} // namespace mawmaw::core

// ============================================================================
// Global state and signal handling
// ============================================================================

static std::atomic<bool> g_running{true};
static std::mutex g_cv_mutex;
static std::condition_variable g_cv;
static std::atomic<bool> g_reload_requested{false};

static void handle_signal(int)
{
    g_running = false;
    g_cv.notify_one();
}

static std::time_t get_file_mtime(const std::string &path)
{
    try
    {
        auto ftime = fs::last_write_time(path);
        auto sctp = std::chrono::time_point_cast<std::chrono::system_clock::duration>(
            ftime - fs::file_time_type::clock::now() + std::chrono::system_clock::now());
        return std::chrono::system_clock::to_time_t(sctp);
    }
    catch (...)
    {
        return 0;
    }
}

// ============================================================================
// Pipeline runtime state
// ============================================================================

struct PipelineState
{
    std::shared_ptr<mawmaw::core::StreamRouter> router;
    std::shared_ptr<mawmaw::publisher::Publisher> publisher;
    std::unique_ptr<mawmaw::core::WindowRegistry> registry_holder;
    mawmaw::core::WindowRegistry *registry = nullptr;
    std::unique_ptr<mawmaw::core::Telemetry> telemetry;
    std::vector<std::thread> script_threads;
    std::vector<mawmaw::ingestor::PluginHandle> plugins;
};

// ============================================================================
// Pipeline builder
// ============================================================================

static PipelineState start_pipeline(
    const mawmaw::config::Config &cfg,
    mawmaw::executor::python::PythonEngine &py_engine,
    mawmaw::executor::wasm::WasmEngine &wasm_engine)
{
    PipelineState state;
    state.registry_holder = std::make_unique<mawmaw::core::WindowRegistry>();
    state.registry = state.registry_holder.get();

    auto router_slot = std::make_shared<std::shared_ptr<mawmaw::core::StreamRouter>>();

    auto pub = std::make_shared<mawmaw::publisher::Publisher>(
        [router_slot](mawmaw::core::Event ev)
        {
            if (*router_slot)
                (*router_slot)->route(std::move(ev));
        });

    std::weak_ptr<mawmaw::publisher::Publisher> weak_pub = pub;
    auto router = std::make_shared<mawmaw::core::StreamRouter>(
        *state.registry,
        [weak_pub](mawmaw::core::ScriptOutput /*out*/)
        {
            // no‑op – script threads will route emitted events directly
        });

    *router_slot = router;
    state.router = router;
    state.publisher = pub;

    // ---- Set up explicit routing ----
    router->set_publisher(pub.get());
    auto routes = mawmaw::config::parse_routes(cfg);
    router->set_routes(routes);

    // ---- Create telemetry ----
    state.telemetry = std::make_unique<mawmaw::core::Telemetry>(*router, *state.registry, *pub);
    state.telemetry->set_enabled(true);
    router->set_telemetry(state.telemetry.get());
    pub->set_telemetry(state.telemetry.get());
    state.registry->set_telemetry(state.telemetry.get());

    // ---- Create publisher endpoints ----
    auto pub_sections = cfg.of_type("publisher");
    if (pub_sections.empty())
    {
        std::cerr << "[warn] no [publisher] sections — adding default stdout and telemetry\n";
        pub->add_endpoint(std::make_shared<mawmaw::publisher::StdoutEndpoint>("stdout"));
        pub->add_endpoint(std::make_shared<mawmaw::publisher::TelemetryStdoutEndpoint>("telemetry"));
    }
    else
    {
        bool has_telemetry = false;
        for (const auto *sec : pub_sections)
        {
            try
            {
                auto ep = mawmaw::publisher::make_endpoint(*sec);
                pub->add_endpoint(ep);
                std::cout << "Publisher: [" << sec->get("type") << "] id=" << ep->name() << "\n";
                if (sec->get("type") == "telemetry")
                    has_telemetry = true;
            }
            catch (const std::exception &e)
            {
                std::cerr << "Publisher failed: " << e.what() << "\n";
            }
        }
        if (!has_telemetry)
        {
            std::cout << "[telemetry] adding default telemetry stdout endpoint\n";
            pub->add_endpoint(std::make_shared<mawmaw::publisher::TelemetryStdoutEndpoint>("telemetry"));
        }
    }

    // ---- Create scripts ----
    struct ScriptThreadData
    {
        size_t registry_idx;
        size_t queue_idx;
        std::string output_stream;    // from emits (first value)
        std::function<mawmaw::core::ScriptOutput(const mawmaw::core::Event &)> dispatch;
    };
    std::vector<ScriptThreadData> script_data;
    static const std::vector<std::string> no_windows;

    for (const auto *sec : cfg.of_type("script"))
    {
        const std::string &id = sec->get("id");
        const std::string &runtime = sec->get("runtime");
        const std::string &path = sec->get("path");
        const std::string &trigger = sec->get("trigger");

        // Extract first value from emits (if any)
        std::string output_stream;
        auto emits_vec = mawmaw::config::get_script_emits(*sec);
        if (!emits_vec.empty())
            output_stream = emits_vec[0];

        if (id.empty() || runtime.empty() || path.empty() || trigger.empty())
        {
            std::cerr << "[script] missing required field — skipping\n";
            continue;
        }
        try
        {
            std::shared_ptr<mawmaw::executor::IScript> script;
            if (runtime == "python")
                script = std::make_shared<mawmaw::executor::python::PythonScript>(id, path);
            else if (runtime == "wasm")
                script = std::make_shared<mawmaw::executor::wasm::WasmScript>(id, path, wasm_engine);
            else
            {
                std::cerr << "[script] unknown runtime '" << runtime << "'\n";
                continue;
            }

            state.telemetry->register_script_stats(script->id());

            mawmaw::core::Subscription sub;
            sub.trigger_stream = trigger;
            const auto &wspecs = sec->multi.count("window") ? sec->multi.at("window") : no_windows;
            for (const auto &wval : wspecs)
            {
                try
                {
                    auto w = mawmaw::config::parse_window(wval);
                    mawmaw::core::WindowSpec spec;
                    spec.type = w.time_based ? mawmaw::core::WindowSpec::Type::TimeBased
                                             : mawmaw::core::WindowSpec::Type::CountBased;
                    spec.duration_ns = w.time_based ? w.n : 0;
                    spec.count = w.time_based ? 256 : static_cast<size_t>(w.n);
                    sub.windows[w.stream_id] = spec;
                }
                catch (const std::exception &e)
                {
                    std::cerr << "[script] bad window '" << wval << "': " << e.what() << "\n";
                }
            }
            if (sub.windows.empty())
            {
                sub.windows[trigger] = mawmaw::core::WindowSpec{
                    mawmaw::core::WindowSpec::Type::CountBased, 0, 64};
            }

            size_t script_idx = state.registry->register_script();
            size_t queue_idx = state.router->register_script_queue();

            auto dispatch = [script, registry = state.registry, sub, script_idx,
                             telemetry = state.telemetry.get()](
                                const mawmaw::core::Event &trigger_ev) -> mawmaw::core::ScriptOutput
            {
                auto start = std::chrono::steady_clock::now();
                mawmaw::core::ScriptOutput out;
                if (script->mode() == mawmaw::core::ScriptMode::ZeroCopy)
                {
                    auto input = registry->make_zero_copy_input(trigger_ev, sub.windows);
                    uint64_t min_seq = UINT64_MAX;
                    for (auto &[sid, view] : input.windows)
                        if (view.begin_seq < min_seq)
                            min_seq = view.begin_seq;
                    if (min_seq != UINT64_MAX)
                        registry->script_begin_read(script_idx, min_seq);
                    out = script->invoke_zero_copy(input);
                    registry->script_end_read(script_idx);
                }
                else
                {
                    out = script->invoke_snapshot(
                        registry->make_snapshot_input(trigger_ev, sub.windows, script_idx));
                }
                auto end = std::chrono::steady_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
                if (telemetry)
                    telemetry->record_script_invocation(script->id(), duration);
                return out;
            };

            router->register_handler(script->id(), std::move(sub), dispatch, queue_idx);
            script_data.push_back({script_idx, queue_idx, output_stream, std::move(dispatch)});
            std::cout << "Script: [" << runtime << "] id=" << id << " trigger=" << trigger
                      << " output_as=" << (output_stream.empty() ? "(same as script)" : output_stream)
                      << " (threaded, zero-copy=" << (script->mode() == mawmaw::core::ScriptMode::ZeroCopy) << ")\n";
        }
        catch (const std::exception &e)
        {
            std::cerr << "[script] '" << id << "' load failed: " << e.what() << "\n";
        }
    }

    // ---- Start script threads ----
    for (auto &data : script_data)
    {
        state.script_threads.emplace_back([router, data] {
            while (g_running) {
                auto ev_opt = router->wait_for_trigger(data.queue_idx);
                if (!ev_opt) break;
                auto out = data.dispatch(*ev_opt);
                // Rename emitted streams if output_stream is configured
                if (!data.output_stream.empty()) {
                    for (auto& ev : out.emitted) {
                        std::strncpy(ev.stream_id, data.output_stream.c_str(),
                                     sizeof(ev.stream_id) - 1);
                        ev.stream_id[sizeof(ev.stream_id) - 1] = '\0';
                        router->route(std::move(ev));
                    }
                } else {
                    for (auto& ev : out.emitted) {
                        router->route(std::move(ev));
                    }
                }
            }
        });
    }

    // ---- Load and start ingestors ----
    for (const auto *sec : cfg.of_type("ingestor"))
    {
        const std::string &id = sec->get("id");
        const std::string &plugin = sec->get("plugin");
        std::string rename_to = sec->get("as");

        if (plugin.empty())
        {
            std::cerr << "[ingestor] missing 'plugin' — skipping\n";
            continue;
        }
        try
        {
            auto handle = mawmaw::ingestor::PluginHandle::load(plugin);
            std::cout << "Ingestor: [" << handle->name() << " v" << handle->version()
                      << "] id=" << id << " plugin=" << plugin;
            if (!rename_to.empty())
                std::cout << " (renaming stream to '" << rename_to << "')";
            std::cout << "\n";

            handle->start([router, rename_to](mawmaw::core::Event ev) {
                if (!rename_to.empty()) {
                    std::strncpy(ev.stream_id, rename_to.c_str(),
                                 sizeof(ev.stream_id) - 1);
                    ev.stream_id[sizeof(ev.stream_id) - 1] = '\0';
                }
                router->route(std::move(ev));
            });

            state.plugins.push_back(std::move(handle));
        }
        catch (const std::exception &e)
        {
            std::cerr << "[ingestor] '" << id << "' failed: " << e.what() << "\n";
        }
    }

    if (state.plugins.empty())
    {
        throw std::runtime_error("No ingestors loaded");
    }

    state.telemetry->start();
    return state;
}

// ============================================================================
// STOP pipeline
// ============================================================================

static void stop_pipeline(PipelineState &state)
{
    if (state.router)
        state.router->set_telemetry(nullptr);

    state.plugins.clear();

    if (state.telemetry)
    {
        state.telemetry->stop();
    }

    if (state.router)
        state.router->stop_all_queues();

    for (auto &t : state.script_threads)
        if (t.joinable())
            t.join();
    state.script_threads.clear();

    if (state.telemetry)
    {
        state.telemetry.reset();
    }

    state.registry_holder.reset();
    state.router.reset();
    state.publisher.reset();
}

// ============================================================================
// Reload monitor
// ============================================================================

static void reload_monitor(const std::string &config_path)
{
    auto get_plugin_paths = [&]() -> std::vector<std::string>
    {
        std::vector<std::string> paths;
        try
        {
            auto cfg = mawmaw::config::parse(config_path);
            for (auto *sec : cfg.of_type("ingestor"))
            {
                std::string p = sec->get("plugin");
                if (!p.empty())
                    paths.push_back(p);
            }
        }
        catch (...)
        {
        }
        return paths;
    };

    auto plugin_paths = get_plugin_paths();
    std::unordered_map<std::string, std::time_t> plugin_mtimes;
    for (const auto &p : plugin_paths)
        plugin_mtimes[p] = get_file_mtime(p);
    std::time_t config_mtime = get_file_mtime(config_path);

    while (g_running)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        std::time_t new_cfg_mtime = get_file_mtime(config_path);
        if (new_cfg_mtime != config_mtime)
        {
            std::cout << "[reload] Config file changed\n";
            g_reload_requested = true;
            config_mtime = new_cfg_mtime;
            plugin_paths = get_plugin_paths();
            for (auto it = plugin_mtimes.begin(); it != plugin_mtimes.end();)
            {
                if (std::find(plugin_paths.begin(), plugin_paths.end(), it->first) == plugin_paths.end())
                    it = plugin_mtimes.erase(it);
                else
                    ++it;
            }
            for (const auto &p : plugin_paths)
                if (plugin_mtimes.find(p) == plugin_mtimes.end())
                    plugin_mtimes[p] = get_file_mtime(p);
            g_cv.notify_one();
            continue;
        }

        for (auto &[path, mtime] : plugin_mtimes)
        {
            std::time_t new_mtime = get_file_mtime(path);
            if (new_mtime != mtime)
            {
                std::cout << "[reload] Plugin file changed: " << path << "\n";
                g_reload_requested = true;
                mtime = new_mtime;
                g_cv.notify_one();
                break;
            }
        }
    }
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char *argv[])
{
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);

    const std::string config_path = (argc > 1) ? argv[1] : "mawmaw.conf";

    mawmaw::executor::python::PythonEngine py_engine;
    py_engine.add_to_path("./scripts");
    mawmaw::executor::wasm::WasmEngine wasm_engine;
    std::cout << "Python ready\n";
    std::cout << "wasm3 ready\n";

    mawmaw::config::Config cfg;
    try
    {
        cfg = mawmaw::config::parse(config_path);
        std::cout << "MAWMAW loaded config: " << config_path << "\n";
    }
    catch (const std::exception &e)
    {
        std::cerr << "Config error: " << e.what() << "\n";
        return 1;
    }

    PipelineState state;
    try
    {
        state = start_pipeline(cfg, py_engine, wasm_engine);
    }
    catch (const std::exception &e)
    {
        std::cerr << "Failed to start pipeline: " << e.what() << "\n";
        return 1;
    }

    std::cout << "\nPipeline running. Ctrl-C to stop.\n\n";

    std::thread monitor_thread(reload_monitor, config_path);

    while (g_running)
    {
        std::unique_lock<std::mutex> lock(g_cv_mutex);
        g_cv.wait_for(lock, std::chrono::milliseconds(500));

        if (!g_running)
            break;

        if (g_reload_requested.exchange(false))
        {
            std::cout << "\n[reload] Performing pipeline reload...\n";
            stop_pipeline(state);
            std::cout << "[reload] Pipeline stopped.\n";

            mawmaw::config::Config new_cfg;
            try
            {
                new_cfg = mawmaw::config::parse(config_path);
                std::cout << "[reload] Config reloaded.\n";
            }
            catch (const std::exception &e)
            {
                std::cerr << "[reload] Failed to parse new config: " << e.what() << "\n";
                std::cerr << "[reload] Exiting.\n";
                g_running = false;
                break;
            }

            try
            {
                state = start_pipeline(new_cfg, py_engine, wasm_engine);
                cfg = std::move(new_cfg);
                std::cout << "[reload] Pipeline restarted successfully.\n\n";
            }
            catch (const std::exception &e)
            {
                std::cerr << "[reload] Failed to start new pipeline: " << e.what() << "\n";
                std::cerr << "[reload] Exiting.\n";
                g_running = false;
                break;
            }
        }
    }

    std::cout << "\nShutting down...\n";
    g_running = false;
    g_cv.notify_all();
    if (monitor_thread.joinable())
        monitor_thread.join();

    stop_pipeline(state);
    std::cout << "MAWMAW stopped cleanly.\n";
    return 0;
}