#pragma once
#include "publisher/publisher.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

// ── websocket — broadcasts every event to all connected WS clients ─────────
//
// Config:
//   [publisher]
//   id   = ws
//   type = websocket
//   host = 0.0.0.0      (optional, default 0.0.0.0)
//   port = 9001          (required)
//
// Every mawmaw::core::Event is serialised to a small JSON object and pushed
// as a WebSocket *text* frame to every currently-connected client:
//
//   {"ts":1737059123456789,"seq":42,"lineage":0,
//    "stream":"trades","schema":"tick_v1","payload_b64":"..."}
//
// This is a plain RFC 6455 implementation over raw POSIX sockets — no TLS,
// no permessage-deflate, no subprotocol negotiation — deliberately, to keep
// the same "no extra dependencies" footprint as the rest of mawmaw (only
// Python3 and the wasm3 submodule are external). SHA-1 and base64 are
// hand-rolled below purely to compute Sec-WebSocket-Accept during the
// handshake.
namespace mawmaw::publisher {

namespace detail {

// ---- minimal SHA-1 (RFC 3174) — only used for the WS handshake ----
struct Sha1 {
    uint32_t h[5] = {0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0};

    static uint32_t rol(uint32_t v, int b) { return (v << b) | (v >> (32 - b)); }

    void process_block(const uint8_t* p) {
        uint32_t w[80];
        for (int i = 0; i < 16; ++i)
            w[i] = (uint32_t(p[i*4]) << 24) | (uint32_t(p[i*4+1]) << 16) |
                   (uint32_t(p[i*4+2]) << 8) | uint32_t(p[i*4+3]);
        for (int i = 16; i < 80; ++i)
            w[i] = rol(w[i-3] ^ w[i-8] ^ w[i-14] ^ w[i-16], 1);

        uint32_t a = h[0], b = h[1], c = h[2], d = h[3], e = h[4];
        for (int i = 0; i < 80; ++i) {
            uint32_t f, k;
            if (i < 20)      { f = (b & c) | ((~b) & d);        k = 0x5A827999; }
            else if (i < 40) { f = b ^ c ^ d;                   k = 0x6ED9EBA1; }
            else if (i < 60) { f = (b & c) | (b & d) | (c & d); k = 0x8F1BBCDC; }
            else             { f = b ^ c ^ d;                   k = 0xCA62C1D6; }
            uint32_t tmp = rol(a, 5) + f + e + k + w[i];
            e = d; d = c; c = rol(b, 30); b = a; a = tmp;
        }
        h[0] += a; h[1] += b; h[2] += c; h[3] += d; h[4] += e;
    }

    // Returns the 20-byte digest for `msg`.
    static void digest(const std::string& msg, uint8_t out[20]) {
        Sha1 ctx;
        std::vector<uint8_t> buf(msg.begin(), msg.end());
        uint64_t bitlen = uint64_t(msg.size()) * 8;
        buf.push_back(0x80);
        while (buf.size() % 64 != 56) buf.push_back(0);
        for (int i = 7; i >= 0; --i) buf.push_back(uint8_t(bitlen >> (i * 8)));
        for (size_t off = 0; off < buf.size(); off += 64) ctx.process_block(&buf[off]);
        for (int i = 0; i < 5; ++i) {
            out[i*4]   = uint8_t(ctx.h[i] >> 24);
            out[i*4+1] = uint8_t(ctx.h[i] >> 16);
            out[i*4+2] = uint8_t(ctx.h[i] >> 8);
            out[i*4+3] = uint8_t(ctx.h[i]);
        }
    }
};

inline std::string base64_encode(const uint8_t* data, size_t len) {
    static const char* tbl = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(((len + 2) / 3) * 4);
    size_t i = 0;
    for (; i + 2 < len; i += 3) {
        uint32_t n = (uint32_t(data[i]) << 16) | (uint32_t(data[i+1]) << 8) | data[i+2];
        out += tbl[(n >> 18) & 0x3F]; out += tbl[(n >> 12) & 0x3F];
        out += tbl[(n >> 6) & 0x3F];  out += tbl[n & 0x3F];
    }
    if (len - i == 1) {
        uint32_t n = uint32_t(data[i]) << 16;
        out += tbl[(n >> 18) & 0x3F]; out += tbl[(n >> 12) & 0x3F]; out += "==";
    } else if (len - i == 2) {
        uint32_t n = (uint32_t(data[i]) << 16) | (uint32_t(data[i+1]) << 8);
        out += tbl[(n >> 18) & 0x3F]; out += tbl[(n >> 12) & 0x3F]; out += tbl[(n >> 6) & 0x3F]; out += "=";
    }
    return out;
}

// Computes Sec-WebSocket-Accept from a client's Sec-WebSocket-Key (RFC 6455 §1.3).
inline std::string ws_accept_key(const std::string& client_key) {
    static const std::string guid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    uint8_t digest[20];
    Sha1::digest(client_key + guid, digest);
    return base64_encode(digest, 20);
}

// Sends `payload` as a single unmasked WS frame (server→client frames must
// NOT be masked, per RFC 6455 §5.1). opcode 0x1 = text, 0x8 = close, 0xA = pong.
inline bool ws_send_frame(int fd, uint8_t opcode, const uint8_t* payload, size_t len) {
    std::vector<uint8_t> frame;
    frame.push_back(0x80 | (opcode & 0x0F)); // FIN=1
    if (len <= 125) {
        frame.push_back(uint8_t(len));
    } else if (len <= 0xFFFF) {
        frame.push_back(126);
        frame.push_back(uint8_t(len >> 8));
        frame.push_back(uint8_t(len));
    } else {
        frame.push_back(127);
        for (int i = 7; i >= 0; --i) frame.push_back(uint8_t(uint64_t(len) >> (i * 8)));
    }
    frame.insert(frame.end(), payload, payload + len);
    size_t sent = 0;
    while (sent < frame.size()) {
        ssize_t n = ::send(fd, frame.data() + sent, frame.size() - sent, MSG_NOSIGNAL);
        if (n <= 0) return false;
        sent += size_t(n);
    }
    return true;
}

// Minimal blocking read of exactly `n` bytes. Returns false on EOF/error.
inline bool read_exact(int fd, uint8_t* buf, size_t n) {
    size_t got = 0;
    while (got < n) {
        ssize_t r = ::recv(fd, buf + got, n - got, 0);
        if (r <= 0) return false;
        got += size_t(r);
    }
    return true;
}

// Reads and discards/handles one client→server frame. Client frames are
// always masked (RFC 6455 §5.3). Returns false when the connection should
// close (close frame, error, or EOF).
inline bool ws_handle_incoming_frame(int fd) {
    uint8_t hdr[2];
    if (!read_exact(fd, hdr, 2)) return false;
    bool     masked  = (hdr[1] & 0x80) != 0;
    uint8_t  opcode  = hdr[0] & 0x0F;
    uint64_t len     = hdr[1] & 0x7F;

    if (len == 126) {
        uint8_t ext[2];
        if (!read_exact(fd, ext, 2)) return false;
        len = (uint64_t(ext[0]) << 8) | ext[1];
    } else if (len == 127) {
        uint8_t ext[8];
        if (!read_exact(fd, ext, 8)) return false;
        len = 0;
        for (int i = 0; i < 8; ++i) len = (len << 8) | ext[i];
    }

    uint8_t mask_key[4] = {};
    if (masked && !read_exact(fd, mask_key, 4)) return false;

    std::vector<uint8_t> payload(len);
    if (len > 0 && !read_exact(fd, payload.data(), len)) return false;
    if (masked)
        for (uint64_t i = 0; i < len; ++i) payload[i] ^= mask_key[i % 4];

    switch (opcode) {
        case 0x8: // close
            ws_send_frame(fd, 0x8, payload.data(), payload.size());
            return false;
        case 0x9: // ping -> pong
            ws_send_frame(fd, 0xA, payload.data(), payload.size());
            return true;
        default:  // text/binary/pong from client — this endpoint is push-only, ignore
            return true;
    }
}

// Parses the HTTP upgrade request and returns the Sec-WebSocket-Key value,
// or empty string if this isn't a valid WS upgrade request.
inline std::string parse_ws_key(int fd) {
    std::string req;
    char c;
    // Read headers up to the blank line terminator ("\r\n\r\n").
    while (req.size() < 8192) {
        ssize_t n = ::recv(fd, &c, 1, 0);
        if (n <= 0) return "";
        req += c;
        if (req.size() >= 4 && req.compare(req.size() - 4, 4, "\r\n\r\n") == 0) break;
    }
    const std::string needle = "Sec-WebSocket-Key:";
    size_t pos = req.find(needle);
    if (pos == std::string::npos) return "";
    pos += needle.size();
    size_t end = req.find("\r\n", pos);
    std::string key = req.substr(pos, end - pos);
    size_t a = key.find_first_not_of(" \t");
    size_t b = key.find_last_not_of(" \t");
    if (a == std::string::npos) return "";
    return key.substr(a, b - a + 1);
}

} // namespace detail

class WebSocketEndpoint final : public IEndpoint {
public:
    WebSocketEndpoint(const std::string& id, const std::string& host, uint16_t port)
        : id_(id)
    {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd_ < 0) throw std::runtime_error("websocket: socket() failed");

        int opt = 1;
        ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port   = htons(port);
        addr.sin_addr.s_addr = (host == "0.0.0.0" || host.empty())
                                    ? INADDR_ANY
                                    : inet_addr(host.c_str());

        if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            ::close(listen_fd_);
            throw std::runtime_error("websocket: bind() failed on " + host + ":" + std::to_string(port));
        }
        if (::listen(listen_fd_, 16) < 0) {
            ::close(listen_fd_);
            throw std::runtime_error("websocket: listen() failed");
        }

        std::cout << "[websocket] '" << id_ << "' listening on " << host << ":" << port << "\n";
        running_ = true;
        accept_thread_ = std::thread(&WebSocketEndpoint::accept_loop, this);
    }

    ~WebSocketEndpoint() override {
        running_ = false;
        if (listen_fd_ >= 0) ::shutdown(listen_fd_, SHUT_RDWR), ::close(listen_fd_);
        if (accept_thread_.joinable()) accept_thread_.join();

        std::lock_guard<std::mutex> lock(clients_mu_);
        for (int fd : clients_) { ::shutdown(fd, SHUT_RDWR); ::close(fd); }
    }

    std::string name() const override { return id_; }

    // Called from the router/publisher thread for every emitted event.
    void publish(const core::Event& ev) override {
        std::string json = to_json(ev);
        std::lock_guard<std::mutex> lock(clients_mu_);
        for (auto it = clients_.begin(); it != clients_.end();) {
            if (!detail::ws_send_frame(*it, 0x1 /* text */,
                                        reinterpret_cast<const uint8_t*>(json.data()),
                                        json.size())) {
                ::close(*it);
                it = clients_.erase(it);
            } else {
                ++it;
            }
        }
    }

private:
    static std::string to_json(const core::Event& ev) {
        std::string payload_b64 = detail::base64_encode(ev.payload, ev.payload_size);
        std::ostringstream os;
        os << "{"
           << "\"ts\":"      << ev.timestamp_ns  << ","
           << "\"seq\":"     << ev.sequence       << ","
           << "\"lineage\":" << ev.lineage_depth  << ","
           << "\"stream\":\""<< ev.stream_id      << "\","
           << "\"schema\":\""<< ev.schema_id      << "\","
           << "\"payload_size\":" << ev.payload_size << ","
           << "\"payload_b64\":\"" << payload_b64 << "\""
           << "}";
        return os.str();
    }

    void accept_loop() {
        while (running_) {
            sockaddr_in client_addr{};
            socklen_t   len = sizeof(client_addr);
            int fd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&client_addr), &len);
            if (fd < 0) {
                if (!running_) break;   // listen_fd_ was closed by the destructor
                continue;
            }
            int opt = 1;
            ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
            std::thread(&WebSocketEndpoint::handshake_and_serve, this, fd).detach();
        }
    }

    void handshake_and_serve(int fd) {
        std::string key = detail::parse_ws_key(fd);
        if (key.empty()) { ::close(fd); return; }

        std::string accept = detail::ws_accept_key(key);
        std::ostringstream resp;
        resp << "HTTP/1.1 101 Switching Protocols\r\n"
             << "Upgrade: websocket\r\n"
             << "Connection: Upgrade\r\n"
             << "Sec-WebSocket-Accept: " << accept << "\r\n\r\n";
        std::string resp_str = resp.str();
        if (::send(fd, resp_str.data(), resp_str.size(), MSG_NOSIGNAL) < 0) {
            ::close(fd); return;
        }

        {
            std::lock_guard<std::mutex> lock(clients_mu_);
            clients_.push_back(fd);
        }
        std::cout << "[websocket] '" << id_ << "' client connected (fd=" << fd << ")\n";

        // Block here reading client frames (pings/close) until disconnect.
        // publish() writes to `fd` concurrently from the router thread.
        while (running_ && detail::ws_handle_incoming_frame(fd)) {}

        std::lock_guard<std::mutex> lock(clients_mu_);
        auto it = std::find(clients_.begin(), clients_.end(), fd);
        if (it != clients_.end()) { ::close(*it); clients_.erase(it); }
    }

    std::string id_;
    int         listen_fd_ = -1;
    std::atomic<bool> running_{false};
    std::thread accept_thread_;

    std::mutex       clients_mu_;
    std::vector<int> clients_;
};

} // namespace mawmaw::publisher