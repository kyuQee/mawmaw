#pragma once
#include <cstdint>
#include <fstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>
#include <sstream>

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
    return key == "window" || key == "endpoint" || key == "emits";
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

// ── Parsing helpers for routes, emits, and ingestor renaming ──

inline std::vector<std::string> split_comma(const std::string& s) {
    std::vector<std::string> result;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, ',')) {
        item = detail::trim(item);
        if (!item.empty()) result.push_back(item);
    }
    return result;
}

// Parse [routes] sections into a map: stream → list of target IDs
inline std::unordered_map<std::string, std::vector<std::string>>
parse_routes(const Config& cfg) {
    std::unordered_map<std::string, std::vector<std::string>> routes;
    for (const auto* sec : cfg.of_type("routes")) {
        // Each key is a stream, value is comma‑separated targets
        for (const auto& [key, val] : sec->kv) {
            auto targets = split_comma(val);
            if (!targets.empty()) {
                auto& existing = routes[key];
                existing.insert(existing.end(), targets.begin(), targets.end());
            }
        }
        // Also handle multi‑key routes (unlikely, but we combine)
        auto it = sec->multi.find("routes"); // not used, but for completeness
        // We only use kv above.
    }
    return routes;
}

// Get the list of streams this script emits (from "emits" key, possibly multi‑line)
inline std::vector<std::string> get_script_emits(const Section& sec) {
    std::vector<std::string> result;
    // Check single key first
    if (sec.has("emits")) {
        auto parts = split_comma(sec.get("emits"));
        result.insert(result.end(), parts.begin(), parts.end());
    }
    // Then multi‑key lines (if any)
    auto it = sec.multi.find("emits");
    if (it != sec.multi.end()) {
        for (const auto& val : it->second) {
            auto parts = split_comma(val);
            result.insert(result.end(), parts.begin(), parts.end());
        }
    }
    return result;
}

// Get the optional "as" rename for an ingestor (returns empty string if not set)
inline std::string get_ingestor_as(const Section& sec) {
    return sec.get("as", "");
}

// Parses "stream_id, count|time, N"  (unchanged)
struct ParsedWindow {
    std::string stream_id;
    bool        time_based = false;
    uint64_t    n          = 0;
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