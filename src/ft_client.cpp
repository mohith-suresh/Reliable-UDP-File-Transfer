#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>
#include <sys/uio.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <poll.h>
#ifdef __linux__
#include <linux/errqueue.h>
#endif

#include <errno.h>
#include <atomic>

#include "ft_common.hpp"

static void die(const char* msg) {
    perror(msg);
    std::exit(1);
}

static uint64_t now_us() {
    timeval tv{};
    gettimeofday(&tv, nullptr);
    return static_cast<uint64_t>(tv.tv_sec) * 1000000ULL + tv.tv_usec;
}

static uint64_t gen_session_id() {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dist;
    return dist(gen);
}

struct ClientState {
    int sockfd = -1;
    sockaddr_in srv{};
    socklen_t srv_len = sizeof(srv);
    uint64_t session_id = 0;

    uint32_t chunk_bytes = DEFAULT_CHUNK;
    uint64_t file_size = 0;
    uint32_t chunk_count = 0;

    std::vector<unsigned char> filebuf;

    // optional mmap path
    int file_fd = -1;
    const unsigned char* mapped = nullptr;
    bool use_mmap = false;
    bool use_zerocopy = false;
    unsigned batch_size = 1; // sendmmsg batch size
    bool is_connected = false;
};

static void load_file(const char* path, ClientState& st) {
    std::ifstream ifs(path, std::ios::binary);
    if (!ifs) die("open input");
    ifs.seekg(0, std::ios::end);
    st.file_size = static_cast<uint64_t>(ifs.tellg());
    ifs.seekg(0, std::ios::beg);
    st.chunk_count = static_cast<uint32_t>((st.file_size + st.chunk_bytes - 1) / st.chunk_bytes);
    st.filebuf.resize(st.file_size);
    if (!st.filebuf.empty()) {
        ifs.read(reinterpret_cast<char*>(st.filebuf.data()), static_cast<std::streamsize>(st.filebuf.size()));
    }
}

static void map_file(const char* path, ClientState& st) {
    st.file_fd = ::open(path, O_RDONLY);
    if (st.file_fd < 0) die("open input mmap");
    struct stat stbuf{};
    if (fstat(st.file_fd, &stbuf) != 0) die("fstat input");
    st.file_size = static_cast<uint64_t>(stbuf.st_size);
    st.chunk_count = static_cast<uint32_t>((st.file_size + st.chunk_bytes - 1) / st.chunk_bytes);
    if (st.file_size > 0) {
        void* p = mmap(nullptr, st.file_size, PROT_READ, MAP_SHARED, st.file_fd, 0);
        if (p == MAP_FAILED) die("mmap input");
        st.mapped = static_cast<const unsigned char*>(p);
        st.use_mmap = true;
    }
}

static void send_info(ClientState& st) {
    InfoMsg info{};
    std::memcpy(info.tag, "INFO", 4);
    info.chunk_bytes = st.chunk_bytes;
    info.file_size = st.file_size;
    info.chunk_count = st.chunk_count;
    info.session_id = st.session_id;

    // Re-send INFO periodically until we receive IACK
    InfoAck ack{};
    auto last_send = std::chrono::steady_clock::now() - std::chrono::milliseconds(1);
    while (true) {
        auto now = std::chrono::steady_clock::now();
        if (now - last_send >= std::chrono::milliseconds(200)) {
            for (int i = 0; i < INFO_REPEATS; ++i) {
                ssize_t rc = st.is_connected
                    ? send(st.sockfd, &info, sizeof(info), 0)
                    : sendto(st.sockfd, &info, sizeof(info), 0, (sockaddr*)&st.srv, st.srv_len);
                if (rc < 0) die("send INFO");
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
            last_send = now;
        }
        pollfd p{}; p.fd = st.sockfd; p.events = POLLIN;
        int pr = ::poll(&p, 1, 300);
        if (pr < 0) die("poll IACK");
        if (pr == 0) continue; // timeout, re-send on next loop
        ssize_t n = recvfrom(st.sockfd, &ack, sizeof(ack), 0, nullptr, nullptr);
        if (n >= (ssize_t)sizeof(ack) && tagEq(ack.tag, "IACK") && ack.session_id == st.session_id) break;
        // otherwise keep waiting / re-sending
    }
}

static void send_pass1(ClientState& st) {
    std::vector<char> pkt(sizeof(DataMsg) + st.chunk_bytes);
    auto* hdr = reinterpret_cast<DataMsg*>(pkt.data());
    std::memcpy(hdr->tag, "DATA", 4);

    uint32_t pkts_since_sleep = 0;
    for (uint32_t seq = 0; seq < st.chunk_count; ++seq) {
        size_t offset = static_cast<size_t>(seq) * st.chunk_bytes;
        size_t remain = st.file_size > offset ? st.file_size - offset : 0;
        size_t to_send = remain < st.chunk_bytes ? remain : st.chunk_bytes;
        hdr->seq = seq;
        hdr->payload = static_cast<uint16_t>(to_send);
        if (to_send > 0) {
            const unsigned char* base = st.use_mmap && st.mapped ? st.mapped : st.filebuf.data();
            std::memcpy(pkt.data() + sizeof(DataMsg), base + offset, to_send);
        }
        ssize_t n = st.is_connected
            ? send(st.sockfd, pkt.data(), sizeof(DataMsg) + to_send, 0)
            : sendto(st.sockfd, pkt.data(), sizeof(DataMsg) + to_send, 0, (sockaddr*)&st.srv, st.srv_len);
        if (n < 0) die("send DATA");
        // Light pacing only every N packets to avoid VM scheduler issues
        if (++pkts_since_sleep >= 256) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
            pkts_since_sleep = 0;
        }
    }

    // Signal end of pass 1
    CtrlMsg c{};
    std::memcpy(c.tag, "P1DN", 4);
    c.session_id = st.session_id;
    for (int i = 0; i < CTRL_REPEATS; ++i) {
        ssize_t rc = st.is_connected
            ? send(st.sockfd, &c, sizeof(c), 0)
            : sendto(st.sockfd, &c, sizeof(c), 0, (sockaddr*)&st.srv, st.srv_len);
        if (rc < 0) die("send P1DN");
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}

static void handle_nacks_and_resend(ClientState& st) {
    // Keep resending requested chunks until DONE
    while (true) {
        char buf[4096 + DEFAULT_CHUNK];
        ssize_t n = recvfrom(st.sockfd, buf, sizeof(buf), 0, nullptr, nullptr);
        if (n < 0) die("recv NACK/DONE");

        if (n >= (ssize_t)sizeof(CtrlMsg)) {
            auto* c = reinterpret_cast<const CtrlMsg*>(buf);
            if (tagEq(c->tag, "DONE") && c->session_id == st.session_id) {
                break;
            }
        }

        if (n >= (ssize_t)sizeof(NackMsgHdr)) {
            auto* h = reinterpret_cast<const NackMsgHdr*>(buf);
            if (!tagEq(h->tag, "NACK")) continue;
            size_t count = h->count;
            if (sizeof(NackMsgHdr) + count * sizeof(uint32_t) > (size_t)n) continue;
            auto* ids = reinterpret_cast<const uint32_t*>(buf + sizeof(NackMsgHdr));

            std::vector<char> pkt(sizeof(DataMsg) + st.chunk_bytes);
            auto* hdr = reinterpret_cast<DataMsg*>(pkt.data());
            std::memcpy(hdr->tag, "DATA", 4);

            size_t pkts2 = 0;
            for (size_t i = 0; i < count; ++i) {
                uint32_t seq = ids[i];
                if (seq >= st.chunk_count) continue;
                size_t offset = static_cast<size_t>(seq) * st.chunk_bytes;
                size_t remain = st.file_size > offset ? st.file_size - offset : 0;
                size_t to_send = remain < st.chunk_bytes ? remain : st.chunk_bytes;
                hdr->seq = seq;
                hdr->payload = static_cast<uint16_t>(to_send);
                if (to_send > 0) {
                    const unsigned char* base = st.use_mmap && st.mapped ? st.mapped : st.filebuf.data();
                    std::memcpy(pkt.data() + sizeof(DataMsg), base + offset, to_send);
                }
                ssize_t sent = st.is_connected
                    ? send(st.sockfd, pkt.data(), sizeof(DataMsg) + to_send, 0)
                    : sendto(st.sockfd, pkt.data(), sizeof(DataMsg) + to_send, 0, (sockaddr*)&st.srv, st.srv_len);
                if (sent < 0) die("send RETRANS");
                if (++pkts2 >= 256) {
                    std::this_thread::sleep_for(std::chrono::microseconds(100));
                    pkts2 = 0;
                }
            }
        }
    }
}

int main(int argc, char** argv) {
    // Optional flags: --chunk N, --rate-mbit X, --verbose, --mmap, --zerocopy, --batch N
    uint32_t opt_chunk = DEFAULT_CHUNK;
    double opt_rate_mbit = 0.0;
    bool opt_verbose = false;
    bool opt_mmap = false;
    bool opt_zerocopy = false;
    unsigned opt_batch = 1;

    int argi = 1;
    auto next_arg = [&](const char* opt) -> const char* {
        if (argi + 1 >= argc) {
            std::cerr << "Missing value for " << opt << "\n";
            std::exit(1);
        }
        return argv[++argi];
    };

    while (argi < argc && std::strncmp(argv[argi], "--", 2) == 0) {
        std::string a = argv[argi];
        if (a == "--chunk") {
            const char* v = next_arg("--chunk");
            opt_chunk = static_cast<uint32_t>(std::stoul(v));
        } else if (a.rfind("--chunk=", 0) == 0) {
            opt_chunk = static_cast<uint32_t>(std::stoul(a.substr(8)));
        } else if (a == "--rate-mbit") {
            const char* v = next_arg("--rate-mbit");
            opt_rate_mbit = std::stod(v);
        } else if (a.rfind("--rate-mbit=", 0) == 0) {
            opt_rate_mbit = std::stod(a.substr(12));
        } else if (a == "--batch") {
            const char* v = next_arg("--batch");
            opt_batch = static_cast<unsigned>(std::stoul(v));
        } else if (a.rfind("--batch=", 0) == 0) {
            opt_batch = static_cast<unsigned>(std::stoul(a.substr(8)));
        } else if (a == "--mmap") {
            opt_mmap = true;
        } else if (a == "--zerocopy") {
            opt_zerocopy = true;
        } else if (a == "--verbose") {
            opt_verbose = true;
        } else if (a == "--help") {
            std::cout << "Usage: " << argv[0]
                      << " [--chunk N] [--rate-mbit X] [--batch N] [--mmap] [--zerocopy] [--verbose] <input_file_path> <server_ip> <server_port>\n";
            return 0;
        } else {
            std::cerr << "Unknown option: " << a << "\n";
            return 1;
        }
        ++argi;
    }
    if (argc - argi != 3) {
        std::cerr << "Usage: " << argv[0]
                  << " [--chunk N] [--rate-mbit X] [--batch N] [--mmap] [--zerocopy] [--verbose] <input_file_path> <server_ip> <server_port>\n";
        return 1;
    }
    const char* in_path = argv[argi++];
    const char* ip = argv[argi++];
    int port = std::stoi(argv[argi++]);

    ClientState st{};
    st.session_id = gen_session_id();
    st.chunk_bytes = std::min<uint32_t>(opt_chunk, 65535u);

    // Choose loading method
    if (opt_mmap) {
        map_file(in_path, st);
    } else {
        load_file(in_path, st);
    }

    st.sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (st.sockfd < 0) die("socket");
    int bufsize = 16 * 1024 * 1024;
    setsockopt(st.sockfd, SOL_SOCKET, SO_SNDBUF, &bufsize, sizeof(bufsize));
    setsockopt(st.sockfd, SOL_SOCKET, SO_RCVBUF, &bufsize, sizeof(bufsize));

    st.srv.sin_family = AF_INET;
    st.srv.sin_port = htons(port);
    if (inet_aton(ip, &st.srv.sin_addr) == 0) die("inet_aton");
    // Connect UDP socket only on Linux when using advanced send paths
#ifdef __linux__
    if (opt_zerocopy || opt_batch > 1) {
        if (connect(st.sockfd, (sockaddr*)&st.srv, sizeof(st.srv)) != 0) die("connect udp");
        st.is_connected = true;
    }
#endif

    // Optional: kernel pacing if supported and requested
    if (opt_rate_mbit > 0.0) {
        unsigned int rate_Bps = static_cast<unsigned int>(opt_rate_mbit * 1000000.0 / 8.0);
        // SO_MAX_PACING_RATE may not exist on non-Linux; ignore errors.
#ifndef SO_MAX_PACING_RATE
#define SO_MAX_PACING_RATE 47
#endif
        if (setsockopt(st.sockfd, SOL_SOCKET, SO_MAX_PACING_RATE, &rate_Bps, sizeof(rate_Bps)) != 0) {
            if (opt_verbose) perror("setsockopt SO_MAX_PACING_RATE (ignored)");
        } else {
            if (opt_verbose) std::cout << "Applied pacing rate ~" << opt_rate_mbit << " Mbit/s\n";
        }
    }
    // Optional zero-copy
    st.use_zerocopy = opt_zerocopy;
#ifdef __linux__
#ifndef SO_ZEROCOPY
#define SO_ZEROCOPY 60
#endif
    if (st.use_zerocopy) {
        int one = 1;
        if (setsockopt(st.sockfd, SOL_SOCKET, SO_ZEROCOPY, &one, sizeof(one)) != 0) {
            if (opt_verbose) perror("SO_ZEROCOPY not supported; falling back");
            st.use_zerocopy = false;
        }
    }
#else
    if (st.use_zerocopy && opt_verbose) {
        std::cerr << "--zerocopy requires Linux; ignoring.\n";
    }
    st.use_zerocopy = false;
#endif
    st.batch_size = opt_batch > 0 ? opt_batch : 1;

    uint64_t t0 = now_us();
    if (opt_verbose) {
        std::cout << "chunk_bytes=" << st.chunk_bytes;
        if (opt_rate_mbit > 0.0) std::cout << ", pacing=" << opt_rate_mbit << " Mbit/s";
        std::cout << "\n";
    }

    send_info(st);
    // If batching or zerocopy requested, use an alternative sender
    if (st.batch_size > 1 || st.use_zerocopy || st.use_mmap) {
        // Batch using sendmmsg when available; otherwise fall back to existing path
#ifdef __linux__
        // Drain ERRQUEUE in background when using SO_ZEROCOPY to release ubufs
        std::atomic<bool> ezc_running{false};
        std::thread ezc;
        if (st.use_zerocopy) {
            ezc_running = true;
            ezc = std::thread([&]() {
                while (ezc_running.load()) {
                    char cmsgbuf[256];
                    struct msghdr msg{};
                    struct iovec iov{};
                    char dummy;
                    iov.iov_base = &dummy;
                    iov.iov_len = sizeof(dummy);
                    msg.msg_iov = &iov;
                    msg.msg_iovlen = 1;
                    msg.msg_control = cmsgbuf;
                    msg.msg_controllen = sizeof(cmsgbuf);
                    ssize_t rn = recvmsg(st.sockfd, &msg, MSG_ERRQUEUE | MSG_DONTWAIT);
                    if (rn < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            std::this_thread::sleep_for(std::chrono::milliseconds(1));
                            continue;
                        } else {
                            std::this_thread::sleep_for(std::chrono::milliseconds(5));
                        }
                    }
                    // Ignore contents; reading frees ubufs
                }
            });
        }
        std::vector<DataMsg> headers;
        headers.resize(st.batch_size);
        std::vector<iovec> iov(st.batch_size * 2);
        std::vector<mmsghdr> msgs(st.batch_size);
        uint32_t seq = 0;
        while (seq < st.chunk_count) {
            unsigned b = 0;
            for (; b < st.batch_size && seq < st.chunk_count; ++b, ++seq) {
                DataMsg& hdr = headers[b];
                std::memcpy(hdr.tag, "DATA", 4);
                hdr.seq = seq;
                size_t offset = static_cast<size_t>(seq) * st.chunk_bytes;
                size_t remain = st.file_size > offset ? st.file_size - offset : 0;
                size_t to_send = remain < st.chunk_bytes ? remain : st.chunk_bytes;
                hdr.payload = static_cast<uint16_t>(to_send);
                // header iov
                iov[2*b].iov_base = &hdr;
                iov[2*b].iov_len  = sizeof(DataMsg);
                // payload iov
                const unsigned char* base = st.use_mmap ? st.mapped : st.filebuf.data();
                iov[2*b+1].iov_base = const_cast<unsigned char*>(base) + offset;
                iov[2*b+1].iov_len  = to_send;
                std::memset(&msgs[b], 0, sizeof(mmsghdr));
                msgs[b].msg_hdr.msg_iov = &iov[2*b];
                msgs[b].msg_hdr.msg_iovlen = (to_send > 0) ? 2 : 1; // handle zero payload gracefully
            }
            int flags = 0;
#ifdef __linux__
            if (st.use_zerocopy) flags |= MSG_ZEROCOPY;
#endif
            int sent = sendmmsg(st.sockfd, msgs.data(), b, flags);
            if (sent < 0) die("sendmmsg DATA");
            // optional light pacing
            if ((seq & 0x3FF) == 0) std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
        if (st.use_zerocopy) {
            ezc_running = false;
            if (ezc.joinable()) ezc.join();
        }
#else
        // Non-Linux: fallback to original path
        send_pass1(st);
#endif
    } else {
        send_pass1(st);
    }
    handle_nacks_and_resend(st);

    // FIN / FACK
    CtrlMsg fin{};
    std::memcpy(fin.tag, "FIN ", 4);
    fin.session_id = st.session_id;
    for (int i = 0; i < CTRL_REPEATS; ++i) {
        ssize_t rc = st.is_connected
            ? send(st.sockfd, &fin, sizeof(fin), 0)
            : sendto(st.sockfd, &fin, sizeof(fin), 0, (sockaddr*)&st.srv, st.srv_len);
        if (rc < 0) die("send FIN");
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }

    // Wait for FACK (best-effort)
    CtrlMsg ack{};
    recvfrom(st.sockfd, &ack, sizeof(ack), 0, nullptr, nullptr);

    uint64_t t1 = now_us();
    double secs = (t1 - t0) / 1e6;
    double mbit = (st.file_size * 8.0) / 1e6;
    double thr = mbit / secs;

    std::cout << "Transfer size = " << mbit << " Mbits\n";
    std::cout << "Elapsed time = " << secs << " sec\n";
    std::cout << "Throughput = " << thr << " Mbit/s\n";

    // Clean up mapping if used
    if (st.use_mmap && st.mapped) {
        munmap(const_cast<unsigned char*>(st.mapped), st.file_size);
        if (st.file_fd >= 0) ::close(st.file_fd);
    }
    close(st.sockfd);
    return 0;
}
