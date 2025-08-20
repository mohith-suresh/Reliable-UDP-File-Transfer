// Early skeleton client: parse args and send a ping datagram
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <iostream>
#include <string>
#include <chrono>
#include <thread>
#include <poll.h>

#include "ft_common.hpp"

static void die(const char* msg) {
    perror(msg);
    std::exit(1);
}

int main(int argc, char** argv) {
    if (argc != 4) {
        std::cerr << "Usage: " << argv[0] << " <input_file_path> <server_ip> <server_port>\n";
        return 1;
    }
    const char* in_path = argv[1];
    (void)in_path; // unused in early skeleton
    const char* ip = argv[2];
    int port = std::stoi(argv[3]);

    int s = socket(AF_INET, SOCK_DGRAM, 0);
    if (s < 0) die("socket");
    sockaddr_in dst{};
    dst.sin_family = AF_INET;
    dst.sin_port = htons(port);
    if (inet_aton(ip, &dst.sin_addr) == 0) die("inet_aton");

    // Handshake: send INFO until IACK
    uint64_t session_id = 0xABCDEF1234567890ULL; // placeholder session id for early step
    InfoMsg info{};
    std::memcpy(info.tag, "INFO", 4);
    info.chunk_bytes = DEFAULT_CHUNK;
    info.file_size = 0;
    info.chunk_count = 0;
    info.session_id = session_id;

    InfoAck ack{};
    auto last_send = std::chrono::steady_clock::now() - std::chrono::milliseconds(1000);
    while (true) {
        auto now = std::chrono::steady_clock::now();
        if (now - last_send >= std::chrono::milliseconds(300)) {
            if (sendto(s, &info, sizeof(info), 0, (sockaddr*)&dst, sizeof(dst)) < 0) die("send INFO");
            last_send = now;
        }
        struct pollfd p{}; p.fd = s; p.events = POLLIN;
        int pr = ::poll(&p, 1, 500);
        if (pr < 0) die("poll IACK");
        if (pr == 0) continue; // retry
        ssize_t rn = recvfrom(s, &ack, sizeof(ack), 0, nullptr, nullptr);
        if (rn >= (ssize_t)sizeof(ack) && tagEq(ack.tag, "IACK") && ack.session_id == session_id) break;
    }
    std::cout << "Handshake complete (IACK)." << std::endl;
    close(s);
    return 0;
}
