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
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <output_file_path> <listen_port>\n";
        return 1;
    }
    const char* out_path = argv[1]; // unused in early skeleton
    (void)out_path;
    int port = std::stoi(argv[2]);

    int s = socket(AF_INET, SOCK_DGRAM, 0);
    if (s < 0) die("socket");

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);
    if (bind(s, (sockaddr*)&addr, sizeof(addr)) < 0) die("bind");

    std::cout << "Server listening on UDP port " << port << "...\n";
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
    std::cout << "Handshake complete: chunks=" << info.chunk_count << ", chunk_bytes=" << info.chunk_bytes << "\n";

    // Open output file and receive pass1 DATA
    int out = ::open(out_path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (out < 0) die("open output");
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
    ::close(out);
    close(s);
    std::cout << "Pass 1 reception complete." << std::endl;
    return 0;
}
