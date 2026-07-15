#pragma once
#include <cstdint>
#include <fstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

// ── MAWMAW config format ───────────────────────────────────────────────────
//
// INI-style. Each section ([ingestor], [script], [publisher]) declares one
// component. Sections of the same type can repeat. Order doesn't matter.
// Comments: lines starting with # or ;
//
// Example:
//   [ingestor]
//   id     = market_feed
//   plugin = ./plugin_fix.so
//
//   [publisher]
//   id   = stdout
//   type = stdout
//
//   [publisher]
//   id   = event_log
//   type = file
//   path = ./output.log
//
//   [script]
//   id      = signal_detector
//   runtime = python
//   path    = ./scripts/signal.py
//   trigger = trades
//   window  = trades, count, 64
//   window  = news, time, 5000000000
//
// Window format: stream_id, count|time, N
//   count — last N events
//   time  — events within last N nanoseconds

namespace mawmaw::config {

struct Section {
    std::string                                               type;
    std::unordered_map<std::string, std::string>              kv;
    std::unordered_map<std::string, std::vector<std::string>> multi;

    const std::string& get(const std::string& key,
                           const std::string& def = "") const {
        auto it = kv.find(key);
        return (it != kv.end()) ? it->second : def;
    }
    bool has(const std::string& key) const { return kv.count(key) > 0; }
};

struct Config {
    std::vector<Section> sections;

    std::vector<const Section*> of_type(const std::string& type) const {
        std::vector<const Section*> r;
        for (auto& s : sections)
            if (s.type == type) r.push_back(&s);
        return r;
    }
};

namespace detail {
inline std::string trim(const std::string& s) {
    auto a = s.find_first_not_of(" \t\r\n");
    if (a == std::string::npos) return "";
    auto b = s.find_last_not_of(" \t\r\n");
    return s.substr(a, b - a + 1);
}
// Keys allowed to appear more than once per section
inline bool is_multi_key(const std::string& key) {
    return key == "window" || key == "endpoint";
}
} // namespace detail

inline Config parse(const std::string& path) {
    std::ifstream f(path);
    if (!f.is_open())
        throw std::runtime_error("Cannot open config: " + path);

    Config cfg;
    Section* current = nullptr;
    std::string line;
    int lineno = 0;

    while (std::getline(f, line)) {
        ++lineno;
        line = detail::trim(line);
        if (line.empty() || line[0] == '#' || line[0] == ';') continue;

        if (line.front() == '[' && line.back() == ']') {
            cfg.sections.push_back({});
            current = &cfg.sections.back();
            current->type = detail::trim(line.substr(1, line.size() - 2));
            continue;
        }

        if (!current)
            throw std::runtime_error("Config line " + std::to_string(lineno) +
                                     ": key=value before any section header");

        auto eq = line.find('=');
        if (eq == std::string::npos)
            throw std::runtime_error("Config line " + std::to_string(lineno) +
                                     ": expected key = value, got: " + line);

        std::string key = detail::trim(line.substr(0, eq));
        std::string val = detail::trim(line.substr(eq + 1));

        if (detail::is_multi_key(key)) {
            current->multi[key].push_back(val);
        } else {
            if (current->kv.count(key))
                throw std::runtime_error("Config line " + std::to_string(lineno) +
                                         ": duplicate key '" + key + "'");
            current->kv[key] = val;
        }
    }
    return cfg;
}

// Parses "stream_id, count|time, N" 
struct ParsedWindow {
    std::string stream_id;
    bool        time_based = false;
    uint64_t    n          = 0;  // event count or nanoseconds
};

inline ParsedWindow parse_window(const std::string& val) {
    std::vector<std::string> parts;
    std::string tok;
    for (char c : val) {
        if (c == ',') { parts.push_back(detail::trim(tok)); tok.clear(); }
        else            tok += c;
    }
    parts.push_back(detail::trim(tok));
    if (parts.size() < 3)
        throw std::runtime_error("window needs 3 fields: stream, count|time, N  (got: " + val + ")");
    ParsedWindow w;
    w.stream_id  = parts[0];
    w.time_based = (parts[1] == "time");
    w.n          = std::stoull(parts[2]);
    return w;
}

} // namespace mawmaw::config
