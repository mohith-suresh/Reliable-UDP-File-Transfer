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
#include <fstream>
#include <vector>

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
    const char* ip = argv[2];
    int port = std::stoi(argv[3]);

    int s = socket(AF_INET, SOCK_DGRAM, 0);
    if (s < 0) die("socket");
    sockaddr_in dst{};
    dst.sin_family = AF_INET;
    dst.sin_port = htons(port);
    if (inet_aton(ip, &dst.sin_addr) == 0) die("inet_aton");

    // Load input file
    std::ifstream ifs(in_path, std::ios::binary);
    if (!ifs) die("open input file");
    ifs.seekg(0, std::ios::end);
    uint64_t file_size = static_cast<uint64_t>(ifs.tellg());
    ifs.seekg(0, std::ios::beg);
    std::vector<unsigned char> filebuf(file_size);
    if (file_size > 0) ifs.read(reinterpret_cast<char*>(filebuf.data()), static_cast<std::streamsize>(file_size));
    uint32_t chunk_bytes = DEFAULT_CHUNK;
    uint32_t chunk_count = static_cast<uint32_t>((file_size + chunk_bytes - 1) / chunk_bytes);

    // Handshake: send INFO until IACK
    uint64_t session_id = 0xABCDEF1234567890ULL; // placeholder session id for early step
    InfoMsg info{};
    std::memcpy(info.tag, "INFO", 4);
    info.chunk_bytes = chunk_bytes;
    info.file_size = file_size;
    info.chunk_count = chunk_count;
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
    std::cout << "Handshake complete (IACK). Starting pass 1..." << std::endl;

    // Pass 1: send each chunk once
    std::vector<char> pkt(sizeof(DataMsg) + chunk_bytes);
    auto* hdr = reinterpret_cast<DataMsg*>(pkt.data());
    std::memcpy(hdr->tag, "DATA", 4);
    for (uint32_t seq = 0; seq < chunk_count; ++seq) {
        size_t offset = static_cast<size_t>(seq) * chunk_bytes;
        size_t remain = file_size > offset ? file_size - offset : 0;
        size_t to_send = remain < chunk_bytes ? remain : chunk_bytes;
        hdr->seq = seq;
        hdr->payload = static_cast<uint16_t>(to_send);
        if (to_send > 0) std::memcpy(pkt.data() + sizeof(DataMsg), filebuf.data() + offset, to_send);
        if (sendto(s, pkt.data(), sizeof(DataMsg) + to_send, 0, (sockaddr*)&dst, sizeof(dst)) < 0) die("send DATA");
        if ((seq & 0xFF) == 0) std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    std::cout << "Pass 1 complete." << std::endl;
    close(s);
    return 0;
}
