// Early skeleton server: parse args, bind UDP, and wait
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <thread>
#include <fcntl.h>
#include <sys/stat.h>

#include "ft_common.hpp"

static void die(const char* msg) {
    perror(msg);
    std::exit(1);
}

int main(int argc, char** argv) {
    bool verbose = false;
    int argi = 1;
    while (argi < argc && std::strncmp(argv[argi], "--", 2) == 0) {
        std::string a = argv[argi];
        if (a == "--verbose") { verbose = true; }
        else if (a == "--help") { std::cout << "Usage: " << argv[0] << " [--verbose] <output_file_path> <listen_port>\n"; return 0; }
        else { std::cerr << "Unknown option: " << a << "\n"; return 1; }
        ++argi;
    }
    if (argc - argi != 2) { std::cerr << "Usage: " << argv[0] << " [--verbose] <output_file_path> <listen_port>\n"; return 1; }
    const char* out_path = argv[argi++];
    int port = std::stoi(argv[argi++]);

    int s = socket(AF_INET, SOCK_DGRAM, 0);
    if (s < 0) die("socket");

    // Increase socket buffers for robustness
    int bufsize = 16 * 1024 * 1024;
    (void)setsockopt(s, SOL_SOCKET, SO_RCVBUF, &bufsize, sizeof(bufsize));
    (void)setsockopt(s, SOL_SOCKET, SO_SNDBUF, &bufsize, sizeof(bufsize));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);
    if (bind(s, (sockaddr*)&addr, sizeof(addr)) < 0) die("bind");

    if (verbose) std::cout << "Server listening on UDP port " << port << "...\n";
    sockaddr_in cli{}; socklen_t cli_len = sizeof(cli);

    // Receive INFO
    InfoMsg info{};
    while (true) {
        ssize_t n = recvfrom(s, &info, sizeof(info), 0, (sockaddr*)&cli, &cli_len);
        if (n < 0) die("recv INFO");
        if (n >= (ssize_t)sizeof(info) && tagEq(info.tag, "INFO")) break;
    }
    // Send IACK burst
    InfoAck ack{}; std::memcpy(ack.tag, "IACK", 4); ack.session_id = info.session_id;
    for (int i = 0; i < 3; ++i) {
        if (sendto(s, &ack, sizeof(ack), 0, (sockaddr*)&cli, cli_len) < 0) die("send IACK");
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if (verbose) std::cout << "Handshake complete: chunks=" << info.chunk_count << ", chunk_bytes=" << info.chunk_bytes << "\n";

    // Open output file and receive pass1 DATA
    int out = ::open(out_path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (out < 0) die("open output");
    // Best-effort pre-size the file
    (void)ftruncate(out, static_cast<off_t>(info.file_size));
    std::vector<char> rbuf(sizeof(DataMsg) + info.chunk_bytes + 64);
    std::vector<uint8_t> received(info.chunk_count, 0);
    auto last_data = std::chrono::steady_clock::now();
    const auto idle = std::chrono::milliseconds(500);
    while (true) {
        // nonblocking-ish loop with short timeout
        fd_set rfds; FD_ZERO(&rfds); FD_SET(s, &rfds);
        timeval tv{}; tv.tv_sec = 0; tv.tv_usec = 200000; // 200ms
        int pr = select(s+1, &rfds, nullptr, nullptr, &tv);
        if (pr < 0) die("select");
        if (pr == 0) {
            if (std::chrono::steady_clock::now() - last_data > idle) break;
            continue;
        }
        ssize_t n = recvfrom(s, rbuf.data(), rbuf.size(), 0, (sockaddr*)&cli, &cli_len);
        if (n < 0) die("recv DATA");
        if (n >= (ssize_t)sizeof(DataMsg)) {
            auto* d = reinterpret_cast<const DataMsg*>(rbuf.data());
            if (!tagEq(d->tag, "DATA")) continue;
            if (d->seq >= info.chunk_count) continue;
            size_t avail = n > (ssize_t)sizeof(DataMsg) ? (size_t)n - sizeof(DataMsg) : 0;
            size_t to_write = std::min<size_t>(avail, d->payload);
            off_t off = static_cast<off_t>(d->seq) * info.chunk_bytes;
            if (to_write > 0) {
                if (::pwrite(out, rbuf.data() + sizeof(DataMsg), to_write, off) < 0) die("pwrite");
                received[d->seq] = 1;
            }
            last_data = std::chrono::steady_clock::now();
        }
    }
    if (verbose) std::cout << "Pass 1 reception complete. Checking for missing chunks..." << std::endl;

    auto compute_missing = [&](std::vector<uint32_t>& out_ids){
        out_ids.clear();
        for (uint32_t i = 0; i < info.chunk_count; ++i) if (!received[i]) out_ids.push_back(i);
    };

    std::vector<uint32_t> missing;
    compute_missing(missing);
    // NACK rounds
    while (!missing.empty()) {
        size_t sent = 0;
        while (sent < missing.size()) {
            size_t batch = std::min<size_t>(256, missing.size() - sent);
            size_t sz = sizeof(NackMsgHdr) + batch * sizeof(uint32_t);
            std::vector<char> pkt(sz);
            auto* h = reinterpret_cast<NackMsgHdr*>(pkt.data());
            std::memcpy(h->tag, "NACK", 4);
            h->count = static_cast<uint16_t>(batch);
            std::memcpy(pkt.data() + sizeof(NackMsgHdr), missing.data() + sent, batch * sizeof(uint32_t));
            if (sendto(s, pkt.data(), pkt.size(), 0, (sockaddr*)&cli, cli_len) < 0) die("send NACK");
            sent += batch;
        }
        // Receive retransmissions for a short window
        auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds(200);
        while (std::chrono::steady_clock::now() < until) {
            fd_set rfds; FD_ZERO(&rfds); FD_SET(s, &rfds);
            timeval tv{}; tv.tv_sec = 0; tv.tv_usec = 50000; // 50ms
            int pr = select(s+1, &rfds, nullptr, nullptr, &tv);
            if (pr < 0) die("select retr");
            if (pr == 0) continue;
            ssize_t n = recvfrom(s, rbuf.data(), rbuf.size(), 0, (sockaddr*)&cli, &cli_len);
            if (n >= (ssize_t)sizeof(DataMsg)) {
                auto* d = reinterpret_cast<const DataMsg*>(rbuf.data());
                if (!tagEq(d->tag, "DATA")) continue;
                if (d->seq >= info.chunk_count) continue;
                size_t avail = n > (ssize_t)sizeof(DataMsg) ? (size_t)n - sizeof(DataMsg) : 0;
                size_t to_write = std::min<size_t>(avail, d->payload);
                off_t off = static_cast<off_t>(d->seq) * info.chunk_bytes;
                if (to_write > 0) {
                    if (::pwrite(out, rbuf.data() + sizeof(DataMsg), to_write, off) < 0) die("pwrite retr");
                    received[d->seq] = 1;
                }
            }
        }
        compute_missing(missing);
    }

    // DONE then FIN/FACK
    CtrlMsg done{}; std::memcpy(done.tag, "DONE", 4); done.session_id = info.session_id;
    for (int i = 0; i < 3; ++i) { if (sendto(s, &done, sizeof(done), 0, (sockaddr*)&cli, cli_len) < 0) die("send DONE"); }
    CtrlMsg fin{};
    while (true) {
        ssize_t n = recvfrom(s, &fin, sizeof(fin), 0, (sockaddr*)&cli, &cli_len);
        if (n < 0) die("recv FIN");
        if (n >= (ssize_t)sizeof(CtrlMsg) && tagEq(fin.tag, "FIN ") && fin.session_id == info.session_id) break;
    }
    CtrlMsg fack{}; std::memcpy(fack.tag, "FACK", 4); fack.session_id = info.session_id;
    for (int i = 0; i < 3; ++i) { if (sendto(s, &fack, sizeof(fack), 0, (sockaddr*)&cli, cli_len) < 0) die("send FACK"); }

    ::close(out);
    close(s);
    if (verbose) std::cout << "Retransmissions complete. Transfer finished." << std::endl;
    return 0;
}
