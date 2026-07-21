#pragma once
#include "publisher/publisher.hpp"
#include "publisher/websocket_endpoint.hpp"
#include "config/config.hpp"
#include <algorithm>
#include <fstream>
#include <iostream>
#include <iomanip>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>
#include <cstring> // for strncmp, memcpy

namespace mawmaw::publisher
{

    // stdout — prints every event to stdout
    class StdoutEndpoint final : public IEndpoint
    {
    public:
        explicit StdoutEndpoint(const std::string &id) : id_(id) {}
        std::string name() const override { return id_; }
        void publish(const core::Event &ev) override
        {
            std::cout
                << "[" << id_ << "]"
                << " stream=" << ev.stream_id
                << " seq=" << ev.sequence
                << " bytes=" << ev.payload_size
                << " lineage=" << ev.lineage_depth
                << "\n";
        }

    private:
        std::string id_;
    };

    // file — appends one line per event to a log file
    class FileEndpoint final : public IEndpoint
    {
    public:
        FileEndpoint(const std::string &id, const std::string &path)
            : id_(id), out_(path, std::ios::app)
        {
            if (!out_.is_open())
                throw std::runtime_error("FileEndpoint: cannot open " + path);
        }
        std::string name() const override { return id_; }
        void publish(const core::Event &ev) override
        {
            out_ << ev.timestamp_ns
                 << " stream=" << ev.stream_id
                 << " seq=" << ev.sequence
                 << " bytes=" << ev.payload_size
                 << " lineage=" << ev.lineage_depth
                 << "\n";
            out_.flush();
        }

    private:
        std::string id_;
        std::ofstream out_;
    };

    // null — silently drops all events
    class NullEndpoint final : public IEndpoint
    {
    public:
        explicit NullEndpoint(const std::string &id) : id_(id) {}
        std::string name() const override { return id_; }
        void publish(const core::Event &) override {}

    private:
        std::string id_;
    };

    // TelemetryStdoutEndpoint – renders the dashboard
    class TelemetryStdoutEndpoint final : public IEndpoint
    {
    public:
        explicit TelemetryStdoutEndpoint(const std::string &id) : id_(id) {}

        std::string name() const override { return id_; }

        void publish(const core::Event &ev) override
        {
            if (std::strncmp(ev.stream_id, "telemetry", 9) != 0 &&
                std::strncmp(ev.stream_id, "telemetry.", 10) != 0)
                return;

            if (std::strncmp(ev.schema_id, "streams_v1", 10) == 0)
            {
                handle_stream_chunk(ev);
            }
            else if (std::strncmp(ev.schema_id, "scripts_v1", 10) == 0)
            {
                handle_script_chunk(ev);
            }
            else
            {
                std::cout << "[telemetry] unknown schema: " << ev.schema_id << "\n";
            }
        }

    private:
        static uint64_t read_u64(const uint8_t *p)
        {
            uint64_t v = 0;
            for (int i = 0; i < 8; ++i)
                v |= (uint64_t)p[i] << (8 * i);
            return v;
        }
        static uint16_t read_u16(const uint8_t *p)
        {
            return (uint16_t)p[0] | ((uint16_t)p[1] << 8);
        }

        struct StreamGroup
        {
            uint64_t timestamp_ns = 0;
            uint64_t total_events = 0;
            uint64_t total_emitted = 0;
            uint64_t drops = 0;
            uint8_t chunk_count = 0;
            std::vector<bool> received;
            std::vector<std::pair<std::string, uint64_t>> entries;
        };
        std::optional<StreamGroup> pending_stream_;

        void handle_stream_chunk(const core::Event &ev)
        {
            const uint8_t *p = ev.payload;
            uint64_t ts = read_u64(p);
            p += 8;
            uint64_t total_events = read_u64(p);
            p += 8;
            uint64_t total_emitted = read_u64(p);
            p += 8;
            uint64_t drops = read_u64(p);
            p += 8;
            uint8_t chunk_index = *p++;
            uint8_t chunk_count = *p++;
            uint16_t n = read_u16(p);
            p += 2;

            if (pending_stream_ && pending_stream_->timestamp_ns != ts)
            {
                pending_stream_.reset();
            }
            if (!pending_stream_)
            {
                pending_stream_ = StreamGroup{};
                pending_stream_->timestamp_ns = ts;
                pending_stream_->total_events = total_events;
                pending_stream_->total_emitted = total_emitted;
                pending_stream_->drops = drops;
                pending_stream_->chunk_count = chunk_count;
                pending_stream_->received.assign(chunk_count, false);
            }
            if (chunk_index >= pending_stream_->received.size() ||
                pending_stream_->received[chunk_index])
            {
                return;
            }
            pending_stream_->received[chunk_index] = true;

            for (uint16_t i = 0; i < n; ++i)
            {
                char stream_id[33] = {};
                std::memcpy(stream_id, p, 32);
                stream_id[32] = '\0';
                p += 32;
                uint64_t count = read_u64(p);
                p += 8;
                pending_stream_->entries.emplace_back(stream_id, count);
            }

            bool complete = std::all_of(pending_stream_->received.begin(),
                                        pending_stream_->received.end(),
                                        [](bool b)
                                        { return b; });
            if (complete)
            {
                std::lock_guard<std::mutex> lock(render_mutex_);
                latest_stream_ = std::move(*pending_stream_);
                pending_stream_.reset();
                render();
            }
        }

        struct ScriptEntry
        {
            std::string id;
            uint64_t count, avg, min, max;
        };
        struct ScriptGroup
        {
            uint64_t timestamp_ns = 0;
            uint8_t chunk_count = 0;
            std::vector<bool> received;
            std::vector<ScriptEntry> entries;
        };
        std::optional<ScriptGroup> pending_script_;

        void handle_script_chunk(const core::Event &ev)
        {
            const uint8_t *p = ev.payload;
            uint64_t ts = read_u64(p);
            p += 8;
            uint8_t chunk_index = *p++;
            uint8_t chunk_count = *p++;
            uint16_t n = read_u16(p);
            p += 2;

            if (pending_script_ && pending_script_->timestamp_ns != ts)
            {
                pending_script_.reset();
            }
            if (!pending_script_)
            {
                pending_script_ = ScriptGroup{};
                pending_script_->timestamp_ns = ts;
                pending_script_->chunk_count = chunk_count;
                pending_script_->received.assign(chunk_count, false);
            }
            if (chunk_index >= pending_script_->received.size() ||
                pending_script_->received[chunk_index])
            {
                return;
            }
            pending_script_->received[chunk_index] = true;

            for (uint16_t i = 0; i < n; ++i)
            {
                char script_id[33] = {};
                std::memcpy(script_id, p, 32);
                script_id[32] = '\0';
                p += 32;
                ScriptEntry e;
                e.id = script_id;
                e.count = read_u64(p);
                p += 8;
                e.avg = read_u64(p);
                p += 8;
                e.min = read_u64(p);
                p += 8;
                e.max = read_u64(p);
                p += 8;
                pending_script_->entries.push_back(std::move(e));
            }

            bool complete = std::all_of(pending_script_->received.begin(),
                                        pending_script_->received.end(),
                                        [](bool b)
                                        { return b; });
            if (complete)
            {
                std::lock_guard<std::mutex> lock(render_mutex_);
                latest_script_ = std::move(*pending_script_);
                pending_script_.reset();
                render();
            }
        }

        void render()
        {
            std::cout << "\033[2J\033[H";
            std::cout << "+------------------------------------------------------------+\n";
            std::cout << "|                 MAWMAW Telemetry Dashboard                  |\n";
            std::cout << "+------------------------------------------------------------+\n";

            if (latest_stream_)
            {
                print_stream_group(*latest_stream_);
            }
            else
            {
                std::cout << "  (no stream metrics yet)\n";
            }

            if (latest_script_)
            {
                print_script_group(*latest_script_);
            }
            else
            {
                std::cout << "  (no script metrics yet)\n";
            }

            std::cout.flush();
        }

        void print_stream_group(const StreamGroup &g) const
        {
            const int id_width = 32;
            const int count_width = 12;

            auto print_sep = [&](char left, char mid, char right, char fill)
            {
                std::cout << left;
                for (int i = 0; i < id_width + 2; ++i)
                    std::cout << fill;
                std::cout << mid;
                for (int i = 0; i < count_width + 2; ++i)
                    std::cout << fill;
                std::cout << right << '\n';
            };

            auto print_row = [&](const std::string &id, uint64_t count)
            {
                std::cout << "| " << std::left << std::setw(id_width) << id
                          << " | " << std::right << std::setw(count_width) << count
                          << " |\n";
            };

            std::cout << "\n  >> STREAM METRICS  (timestamp: " << g.timestamp_ns << " ns)\n";
            std::cout << "-----------------------------------------------------------------\n";
            std::cout << "  total events (in) : " << g.total_events << "\n";
            std::cout << "  total emitted     : " << g.total_emitted << "\n";
            std::cout << "  drops             : " << g.drops << "\n";
            std::cout << "-----------------------------------------------------------------\n";

            if (g.entries.empty())
            {
                std::cout << "  (no per-stream activity)\n";
            }
            else
            {
                print_sep('+', '+', '+', '-');
                std::cout << "| " << std::left << std::setw(id_width) << "Stream ID"
                          << " | " << std::right << std::setw(count_width) << "Count"
                          << " |\n";
                print_sep('+', '+', '+', '-');

                for (const auto &[sid, count] : g.entries)
                {
                    print_row(sid, count);
                }

                print_sep('+', '+', '+', '-');
            }
        }

        void print_script_group(const ScriptGroup &g) const
        {
            const int id_width = 32;
            const int num_width = 10;

            auto print_sep = [&](char left, char mid1, char mid2, char mid3, char mid4, char right, char fill)
            {
                std::cout << left;
                for (int i = 0; i < id_width + 2; ++i)
                    std::cout << fill;
                std::cout << mid1;
                for (int i = 0; i < num_width + 2; ++i)
                    std::cout << fill;
                std::cout << mid2;
                for (int i = 0; i < num_width + 2; ++i)
                    std::cout << fill;
                std::cout << mid3;
                for (int i = 0; i < num_width + 2; ++i)
                    std::cout << fill;
                std::cout << mid4;
                for (int i = 0; i < num_width + 2; ++i)
                    std::cout << fill;
                std::cout << right << '\n';
            };

            auto print_row = [&](const std::string &id, uint64_t count, double avg_us, double min_us, double max_us)
            {
                std::cout << "| " << std::left << std::setw(id_width) << id
                          << " | " << std::right << std::setw(num_width) << count
                          << " | " << std::right << std::setw(num_width) << std::fixed << std::setprecision(2) << avg_us
                          << " | " << std::right << std::setw(num_width) << std::fixed << std::setprecision(2) << min_us
                          << " | " << std::right << std::setw(num_width) << std::fixed << std::setprecision(2) << max_us
                          << " |\n";
            };

            std::cout << "\n  >> SCRIPT METRICS  (timestamp: " << g.timestamp_ns << " ns)\n";
            std::cout << "-----------------------------------------------------------------\n";

            if (g.entries.empty())
            {
                std::cout << "  (no script activity)\n";
            }
            else
            {
                print_sep('+', '+', '+', '+', '+', '+', '-');
                std::cout << "| " << std::left << std::setw(id_width) << "Script ID"
                          << " | " << std::right << std::setw(num_width) << "Calls"
                          << " | " << std::right << std::setw(num_width) << "Avg (µs)"
                          << " | " << std::right << std::setw(num_width) << "Min (µs)"
                          << " | " << std::right << std::setw(num_width) << "Max (µs)"
                          << " |\n";
                print_sep('+', '+', '+', '+', '+', '+', '-');

                for (const auto &e : g.entries)
                {
                    double avg_us = (e.count > 0) ? e.avg / 1000.0 : 0.0;
                    double min_us = (e.count > 0) ? e.min / 1000.0 : 0.0;
                    double max_us = (e.count > 0) ? e.max / 1000.0 : 0.0;
                    print_row(e.id, e.count, avg_us, min_us, max_us);
                }

                print_sep('+', '+', '+', '+', '+', '+', '-');
            }
        }

        std::string id_;
        mutable std::mutex render_mutex_;
        std::optional<StreamGroup> latest_stream_;
        std::optional<ScriptGroup> latest_script_;
    };

    // ── http — serves the latest received event payload via HTTP ──────────────
    //
    // Config:
    //   [publisher]
    //   id   = prometheus
    //   type = http
    //   host = 0.0.0.0        (optional, default 0.0.0.0)
    //   port = 9090            (required)
    //   path = /metrics        (optional, default /)
    //   content_type = text/plain; version=0.0.4   (optional, default text/plain)
    //
    // Every event received via publish() replaces the stored payload.
    // A GET request to the configured path returns that payload with the
    // configured Content-Type header.

    class HttpEndpoint final : public IEndpoint
    {
    public:
        HttpEndpoint(const std::string &id,
                     const std::string &host,
                     uint16_t port,
                     const std::string &path = "/",
                     const std::string &content_type = "text/plain")
            : id_(id), host_(host), port_(port), path_(path), content_type_(content_type)
        {
            listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
            if (listen_fd_ < 0)
                throw std::runtime_error("http: socket() failed");

            int opt = 1;
            ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

            sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_port = htons(port_);
            addr.sin_addr.s_addr = (host_ == "0.0.0.0" || host_.empty())
                                       ? INADDR_ANY
                                       : inet_addr(host_.c_str());

            if (::bind(listen_fd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
            {
                ::close(listen_fd_);
                throw std::runtime_error("http: bind() failed on " + host_ + ":" + std::to_string(port_));
            }
            if (::listen(listen_fd_, 16) < 0)
            {
                ::close(listen_fd_);
                throw std::runtime_error("http: listen() failed");
            }

            std::cout << "[http] '" << id_ << "' listening on " << host_ << ":" << port_
                      << " (path=" << path_ << ", content-type=" << content_type_ << ")\n";
            running_ = true;
            accept_thread_ = std::thread(&HttpEndpoint::accept_loop, this);
        }

        ~HttpEndpoint() override
        {
            running_ = false;
            if (listen_fd_ >= 0)
            {
                ::shutdown(listen_fd_, SHUT_RDWR);
                ::close(listen_fd_);
            }
            if (accept_thread_.joinable())
                accept_thread_.join();

            // Close any remaining client fds
            std::lock_guard<std::mutex> lock(clients_mu_);
            for (int fd : clients_)
            {
                ::shutdown(fd, SHUT_RDWR);
                ::close(fd);
            }
        }

        std::string name() const override { return id_; }

        // Called from the router/publisher thread for every emitted event.
        void publish(const core::Event &ev) override
        {
            std::lock_guard<std::mutex> lock(payload_mu_);
            // Store the entire payload (raw bytes)
            payload_.assign(ev.payload, ev.payload + ev.payload_size);
            // Also store the timestamp for optional logging
            last_update_ = ev.timestamp_ns;
        }

    private:
        // ---------- HTTP server internals ----------
        void accept_loop()
        {
            while (running_)
            {
                sockaddr_in client_addr{};
                socklen_t len = sizeof(client_addr);
                int fd = ::accept(listen_fd_, reinterpret_cast<sockaddr *>(&client_addr), &len);
                if (fd < 0)
                {
                    if (!running_)
                        break;
                    continue;
                }
                // Detach a thread to handle this client
                std::thread(&HttpEndpoint::handle_client, this, fd).detach();
            }
        }

        void handle_client(int fd)
        {
            // Add to active clients list (for cleanup)
            {
                std::lock_guard<std::mutex> lock(clients_mu_);
                clients_.push_back(fd);
            }

            // Read request until blank line
            std::string request;
            char buf[4096];
            ssize_t n;
            while ((n = ::recv(fd, buf, sizeof(buf) - 1, 0)) > 0)
            {
                request.append(buf, n);
                if (request.find("\r\n\r\n") != std::string::npos ||
                    request.find("\n\n") != std::string::npos)
                {
                    break;
                }
            }

            if (n < 0)
            {
                ::close(fd);
                remove_client(fd);
                return;
            }

            // Parse request line
            std::string method, path, version;
            std::istringstream iss(request);
            iss >> method >> path >> version;

            // Build response
            std::string response;
            if (method == "GET" && path == path_)
            {
                // Serve the payload
                std::string payload_copy;
                {
                    std::lock_guard<std::mutex> lock(payload_mu_);
                    payload_copy = payload_;
                }
                response = "HTTP/1.1 200 OK\r\n";
                response += "Content-Type: " + content_type_ + "\r\n";
                response += "Content-Length: " + std::to_string(payload_copy.size()) + "\r\n";
                response += "Connection: close\r\n";
                response += "\r\n";
                response += payload_copy;
            }
            else if (method == "GET" && path == "/health")
            {
                // Optional health check
                response = "HTTP/1.1 200 OK\r\n";
                response += "Content-Type: text/plain\r\n";
                response += "Content-Length: 2\r\n";
                response += "Connection: close\r\n";
                response += "\r\n";
                response += "OK";
            }
            else if (method != "GET")
            {
                response = "HTTP/1.1 405 Method Not Allowed\r\n";
                response += "Allow: GET\r\n";
                response += "Connection: close\r\n";
                response += "\r\n";
            }
            else
            {
                response = "HTTP/1.1 404 Not Found\r\n";
                response += "Connection: close\r\n";
                response += "\r\n";
            }

            ::send(fd, response.data(), response.size(), MSG_NOSIGNAL);
            ::shutdown(fd, SHUT_RDWR);
            ::close(fd);
            remove_client(fd);
        }

        void remove_client(int fd)
        {
            std::lock_guard<std::mutex> lock(clients_mu_);
            auto it = std::find(clients_.begin(), clients_.end(), fd);
            if (it != clients_.end())
                clients_.erase(it);
        }

        // ---------- Member variables ----------
        std::string id_;
        std::string host_;
        uint16_t port_;
        std::string path_;
        std::string content_type_;

        std::atomic<bool> running_{false};
        int listen_fd_ = -1;
        std::thread accept_thread_;

        // Payload storage (protected by mutex)
        std::mutex payload_mu_;
        std::string payload_;      // latest event payload
        uint64_t last_update_ = 0; // timestamp of last update (not used but nice)

        // Active client fds (for cleanup on shutdown)
        std::mutex clients_mu_;
        std::vector<int> clients_;
    };

    // Factory – create an endpoint from a [publisher] config section
    inline std::shared_ptr<IEndpoint> make_endpoint(const config::Section &sec)
    {
        std::string id = sec.get("id");     // copy
        std::string type = sec.get("type"); // copy

        if (id.empty())
            throw std::runtime_error("[publisher] missing 'id'");
        if (type.empty())
            throw std::runtime_error("[publisher] '" + id + "' missing 'type'");

        if (type == "stdout")
            return std::make_shared<StdoutEndpoint>(id);
        if (type == "null")
            return std::make_shared<NullEndpoint>(id);
        if (type == "telemetry")
            return std::make_shared<TelemetryStdoutEndpoint>(id);

        if (type == "file")
        {
            std::string path = sec.get("path"); // copy
            if (path.empty())
                throw std::runtime_error("[publisher] '" + id + "' type=file requires 'path'");
            return std::make_shared<FileEndpoint>(id, path);
        }

        if (type == "websocket")
        {
            std::string port_str = sec.get("port"); // copy
            if (port_str.empty())
                throw std::runtime_error("[publisher] '" + id + "' type=websocket requires 'port'");
            std::string host = sec.get("host", "0.0.0.0"); // copy
            uint16_t port = static_cast<uint16_t>(std::stoi(port_str));
            return std::make_shared<WebSocketEndpoint>(id, host, port);
        }

        if (type == "http")
        {
            std::string host = sec.get("host", "0.0.0.0");
            std::string port_str = sec.get("port");
            if (port_str.empty())
                throw std::runtime_error("[publisher] '" + id + "' type=http requires 'port'");
            uint16_t port = static_cast<uint16_t>(std::stoi(port_str));
            std::string path = sec.get("path", "/");
            std::string content_type = sec.get("content_type", "text/plain");
            return std::make_shared<HttpEndpoint>(id, host, port, path, content_type);
        }

        throw std::runtime_error("[publisher] '" + id + "' unknown type '" + type + "'");
    }

} // namespace mawmaw::publisher