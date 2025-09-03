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
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "ft_common.hpp"

static void die(const char* msg) {
    perror(msg);
    std::exit(1);
}

int main(int argc, char** argv) {
    // Options: --chunk N, --verbose, --rate-mbit X
    uint32_t opt_chunk = DEFAULT_CHUNK;
    bool opt_verbose = false;
    double opt_rate_mbit = 0.0;
    bool opt_mmap = false;
    int argi = 1;
    auto next_val = [&](const char* opt){ if (argi + 1 >= argc) { std::cerr << "Missing value for " << opt << "\n"; std::exit(1);} return argv[++argi]; };
    while (argi < argc && std::strncmp(argv[argi], "--", 2) == 0) {
        std::string a = argv[argi];
        if (a == "--chunk") { opt_chunk = static_cast<uint32_t>(std::stoul(next_val("--chunk"))); }
        else if (a == "--verbose") { opt_verbose = true; }
        else if (a == "--rate-mbit") { opt_rate_mbit = std::stod(next_val("--rate-mbit")); }
        else if (a == "--mmap") { opt_mmap = true; }
        else if (a == "--help") { std::cout << "Usage: " << argv[0] << " [--chunk N] [--verbose] [--rate-mbit X] [--mmap] <input_file_path> <server_ip> <server_port>\n"; return 0; }
        else { std::cerr << "Unknown option: " << a << "\n"; return 1; }
        ++argi;
    }
    if (argc - argi != 3) {
        std::cerr << "Usage: " << argv[0] << " [--chunk N] [--verbose] [--rate-mbit X] [--mmap] <input_file_path> <server_ip> <server_port>\n";
        return 1;
    }
    const char* in_path = argv[argi++];
    const char* ip = argv[argi++];
    int port = std::stoi(argv[argi++]);

    int s = socket(AF_INET, SOCK_DGRAM, 0);
    if (s < 0) die("socket");
    sockaddr_in dst{};
    dst.sin_family = AF_INET;
    dst.sin_port = htons(port);
    if (inet_aton(ip, &dst.sin_addr) == 0) die("inet_aton");

    // Load input file
    uint64_t file_size = 0;
    std::vector<unsigned char> filebuf;
    int file_fd = -1;
    const unsigned char* mapped = nullptr;
    if (opt_mmap) {
        file_fd = ::open(in_path, O_RDONLY);
        if (file_fd < 0) die("open input");
        struct stat st{}; if (fstat(file_fd, &st) != 0) die("fstat input");
        file_size = static_cast<uint64_t>(st.st_size);
        if (file_size > 0) {
            void* p = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, file_fd, 0);
            if (p == MAP_FAILED) die("mmap input");
            mapped = static_cast<const unsigned char*>(p);
        }
    } else {
        std::ifstream ifs(in_path, std::ios::binary);
        if (!ifs) die("open input file");
        ifs.seekg(0, std::ios::end);
        file_size = static_cast<uint64_t>(ifs.tellg());
        ifs.seekg(0, std::ios::beg);
        filebuf.resize(file_size);
        if (file_size > 0) ifs.read(reinterpret_cast<char*>(filebuf.data()), static_cast<std::streamsize>(file_size));
    }
    uint32_t chunk_bytes = opt_chunk;
    uint32_t chunk_count = static_cast<uint32_t>((file_size + chunk_bytes - 1) / chunk_bytes);

    // Optional pacing (best-effort)
    if (opt_rate_mbit > 0.0) {
#ifndef SO_MAX_PACING_RATE
#define SO_MAX_PACING_RATE 47
#endif
        unsigned int rate_Bps = static_cast<unsigned int>(opt_rate_mbit * 1000000.0 / 8.0);
        (void)setsockopt(s, SOL_SOCKET, SO_MAX_PACING_RATE, &rate_Bps, sizeof(rate_Bps));
    }

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
    if (opt_verbose) std::cout << "Handshake complete (IACK). Starting pass 1..." << std::endl;

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
        if (to_send > 0) {
            const unsigned char* base = opt_mmap && mapped ? mapped : filebuf.data();
            std::memcpy(pkt.data() + sizeof(DataMsg), base + offset, to_send);
        }
        if (sendto(s, pkt.data(), sizeof(DataMsg) + to_send, 0, (sockaddr*)&dst, sizeof(dst)) < 0) die("send DATA");
        if ((seq & 0xFF) == 0) std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    if (opt_verbose) std::cout << "Pass 1 complete. Signaling P1DN..." << std::endl;

    // Signal end of pass1
    CtrlMsg p1{}; std::memcpy(p1.tag, "P1DN", 4); p1.session_id = session_id;
    for (int i = 0; i < 3; ++i) { if (sendto(s, &p1, sizeof(p1), 0, (sockaddr*)&dst, sizeof(dst)) < 0) die("send P1DN"); }

    // Handle NACKs and retransmit until DONE
    while (true) {
        char rbuf[4096 + 2048];
        ssize_t rn = recvfrom(s, rbuf, sizeof(rbuf), 0, nullptr, nullptr);
        if (rn < 0) die("recv NACK/DONE");
        if (rn >= (ssize_t)sizeof(CtrlMsg)) {
            auto* c = reinterpret_cast<const CtrlMsg*>(rbuf);
            if (tagEq(c->tag, "DONE") && c->session_id == session_id) break;
        }
        if (rn >= (ssize_t)sizeof(NackMsgHdr)) {
            auto* h = reinterpret_cast<const NackMsgHdr*>(rbuf);
            if (!tagEq(h->tag, "NACK")) continue;
            size_t count = h->count;
            if (sizeof(NackMsgHdr) + count * sizeof(uint32_t) > (size_t)rn) continue;
            auto* ids = reinterpret_cast<const uint32_t*>(rbuf + sizeof(NackMsgHdr));
            for (size_t i = 0; i < count; ++i) {
                uint32_t seq = ids[i];
                if (seq >= chunk_count) continue;
                size_t offset = static_cast<size_t>(seq) * chunk_bytes;
                size_t remain = file_size > offset ? file_size - offset : 0;
                size_t to_send = remain < chunk_bytes ? remain : chunk_bytes;
                hdr->seq = seq;
                hdr->payload = static_cast<uint16_t>(to_send);
                if (to_send > 0) std::memcpy(pkt.data() + sizeof(DataMsg), filebuf.data() + offset, to_send);
                if (sendto(s, pkt.data(), sizeof(DataMsg) + to_send, 0, (sockaddr*)&dst, sizeof(dst)) < 0) die("send RETRANS");
            }
        }
    }

    // FIN / FACK
    CtrlMsg fin{}; std::memcpy(fin.tag, "FIN ", 4); fin.session_id = session_id;
    for (int i = 0; i < 3; ++i) { if (sendto(s, &fin, sizeof(fin), 0, (sockaddr*)&dst, sizeof(dst)) < 0) die("send FIN"); }
    CtrlMsg fack{}; recvfrom(s, &fack, sizeof(fack), 0, nullptr, nullptr);
    if (opt_mmap && mapped) { munmap(const_cast<unsigned char*>(mapped), file_size); if (file_fd >= 0) ::close(file_fd); }
    if (opt_verbose) std::cout << "Transfer complete." << std::endl;
    close(s);
    return 0;
}
