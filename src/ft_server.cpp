#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <thread>
#include <vector>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
#include <poll.h>

#include "ft_common.hpp"

static void die(const char* msg) {
    perror(msg);
    std::exit(1);
}

struct ServerState {
    int sockfd = -1;
    sockaddr_in client{};
    socklen_t client_len = sizeof(client);
    uint64_t session_id = 0;

    // Output file
    int out_fd = -1;
    std::string out_path;

    uint32_t chunk_bytes = DEFAULT_CHUNK;
    uint64_t file_size = 0;
    uint32_t chunk_count = 0;

    std::vector<uint8_t> received;      // per-chunk flags (0/1)

    std::atomic<bool> pass1_done{false};
    std::atomic<bool> receiving{true};

    bool verbose = false;
};

static void send_iack(ServerState& st) {
    InfoAck ack{};
    std::memcpy(ack.tag, "IACK", 4);
    ack.session_id = st.session_id;
    // Send a small burst of IACKs for robustness
    for (int i = 0; i < INFO_REPEATS; ++i) {
        if (sendto(st.sockfd, &ack, sizeof(ack), 0, (sockaddr*)&st.client, st.client_len) < 0) {
            die("sendto IACK");
        }
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}

static void send_ctrl(ServerState& st, const char* tag) {
    CtrlMsg m{};
    std::memcpy(m.tag, tag, 4);
    m.session_id = st.session_id;
    for (int i = 0; i < CTRL_REPEATS; ++i) {
        if (sendto(st.sockfd, &m, sizeof(m), 0, (sockaddr*)&st.client, st.client_len) < 0) {
            die("sendto ctrl");
        }
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}

static void recv_loop(ServerState& st) {
    // Receive INFO first
    while (true) {
        InfoMsg info{};
        ssize_t n = recvfrom(st.sockfd, &info, sizeof(info), 0, (sockaddr*)&st.client, &st.client_len);
        if (n < 0) die("recvfrom INFO");
        if (n >= (ssize_t)sizeof(info) && tagEq(info.tag, "INFO")) {
            st.session_id = info.session_id;
            // Clamp chunk size to a sane UDP payload maximum
            st.chunk_bytes = std::min<uint32_t>(info.chunk_bytes, 65535u);
            st.file_size = info.file_size;
            st.chunk_count = info.chunk_count;
            st.received.assign(st.chunk_count, 0);
            send_iack(st);
            if (st.verbose) {
                std::cout << "INFO received: size=" << st.file_size
                          << ", chunks=" << st.chunk_count
                          << ", chunk_bytes=" << st.chunk_bytes << "\n";
            }
            // Open and pre-size the output file
            st.out_fd = ::open(st.out_path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
            if (st.out_fd < 0) die("open output file");
            // Best-effort pre-size; it's okay if it fails (e.g., some FS)
            if (ftruncate(st.out_fd, static_cast<off_t>(st.file_size)) != 0) {
                // If ftruncate fails (e.g., EINVAL on some FS), continue without pre-sizing
            }
            break;
        }
    }

    // Receive DATA until P1DN or idle timeout (to avoid stalls if P1DN is lost)
    using clock = std::chrono::steady_clock;
    auto last_data = clock::now();
    bool seen_any_data = false;
    const std::chrono::milliseconds idle_threshold(500); // 500 ms of no DATA => end pass 1

    // Allocate a receive buffer large enough for the negotiated chunk size
    std::vector<char> rbuf(sizeof(DataMsg) + st.chunk_bytes + 64);
    while (!st.pass1_done.load()) {
        // Poll with 200 ms timeout
        pollfd pfd{};
        pfd.fd = st.sockfd;
        pfd.events = POLLIN;
        int pr = ::poll(&pfd, 1, 200);
        if (pr < 0) die("poll pass1");
        if (pr == 0) {
            // timeout
            if (seen_any_data && (clock::now() - last_data) >= idle_threshold) {
                st.pass1_done = true;
                break;
            }
            continue;
        }

        ssize_t n = recvfrom(st.sockfd, rbuf.data(), rbuf.size(), 0, (sockaddr*)&st.client, &st.client_len);
        if (n < 0) die("recvfrom DATA/P1DN");

        if (n >= (ssize_t)sizeof(CtrlMsg)) {
            auto* c = reinterpret_cast<const CtrlMsg*>(rbuf.data());
            if (tagEq(c->tag, "P1DN") && c->session_id == st.session_id) {
                st.pass1_done = true;
                break;
            }
        }

        // If duplicate INFO arrives (client missed IACK), re-send IACK and continue
        if (n >= (ssize_t)sizeof(InfoMsg)) {
            auto* inf = reinterpret_cast<const InfoMsg*>(rbuf.data());
            if (tagEq(inf->tag, "INFO") && inf->session_id == st.session_id) {
                send_iack(st);
                continue;
            }
        }

        if (n >= (ssize_t)sizeof(DataMsg)) {
            auto* d = reinterpret_cast<const DataMsg*>(rbuf.data());
            if (!tagEq(d->tag, "DATA")) continue;
            if (d->payload > st.chunk_bytes) continue;
            if (d->seq >= st.chunk_count) continue;
            size_t offset = static_cast<size_t>(d->seq) * st.chunk_bytes;
            // Ensure the full payload is present in this datagram
            size_t available = n > (ssize_t)sizeof(DataMsg) ? static_cast<size_t>(n) - sizeof(DataMsg) : 0;
            size_t to_copy = std::min<size_t>(d->payload, available);
            if (to_copy == d->payload && to_copy > 0) {
                ssize_t w = ::pwrite(st.out_fd, rbuf.data() + sizeof(DataMsg), to_copy, static_cast<off_t>(offset));
                if (w < 0) die("pwrite pass1");
                st.received[d->seq] = 1;
            }
            seen_any_data = true;
            last_data = clock::now();
        }
    }
}

static uint32_t compute_missing(const ServerState& st, std::vector<uint32_t>& out) {
    out.clear();
    out.reserve(2048);
    uint32_t missing = 0;
    for (uint32_t i = 0; i < st.chunk_count; ++i) {
        if (st.received[i] == 0) {
            out.push_back(i);
            ++missing;
        }
    }
    return missing;
}

static void recv_retrans(ServerState& st) {
    // Continue to accept DATA while main thread sends NACK batches
    // Allocate a nonblocking receive buffer matching negotiated chunk size
    std::vector<char> rbuf(sizeof(DataMsg) + st.chunk_bytes + 64);
    while (st.receiving.load()) {
        sockaddr_in tmp{};
        socklen_t tmplen = sizeof(tmp);
        ssize_t n = recvfrom(st.sockfd, rbuf.data(), rbuf.size(), MSG_DONTWAIT, (sockaddr*)&tmp, &tmplen);
        if (n < 0) {
            // No data available, sleep briefly
            std::this_thread::sleep_for(std::chrono::microseconds(200));
            continue;
        }
        if (n >= (ssize_t)sizeof(DataMsg)) {
            auto* d = reinterpret_cast<const DataMsg*>(rbuf.data());
            if (!tagEq(d->tag, "DATA")) continue;
            if (d->payload > st.chunk_bytes) continue;
            if (d->seq >= st.chunk_count) continue;
            size_t offset = static_cast<size_t>(d->seq) * st.chunk_bytes;
            size_t available = n > (ssize_t)sizeof(DataMsg) ? static_cast<size_t>(n) - sizeof(DataMsg) : 0;
            size_t to_copy = std::min<size_t>(d->payload, available);
            if (to_copy == d->payload && to_copy > 0) {
                ssize_t w = ::pwrite(st.out_fd, rbuf.data() + sizeof(DataMsg), to_copy, static_cast<off_t>(offset));
                if (w < 0) die("pwrite retr");
                st.received[d->seq] = 1;
            }
        }
    }
}

int main(int argc, char** argv) {
    // Optional flags: --verbose
    bool opt_verbose = false;
    int argi = 1;
    while (argi < argc && std::strncmp(argv[argi], "--", 2) == 0) {
        std::string a = argv[argi];
        if (a == "--verbose") {
            opt_verbose = true;
        } else if (a == "--help") {
            std::cout << "Usage: " << argv[0] << " [--verbose] <output_file_path> <listen_port>\n";
            return 0;
        } else {
            std::cerr << "Unknown option: " << a << "\n";
            return 1;
        }
        ++argi;
    }
    if (argc - argi != 2) {
        std::cerr << "Usage: " << argv[0] << " [--verbose] <output_file_path> <listen_port>\n";
        return 1;
    }
    const char* out_path = argv[argi++];
    int port = std::stoi(argv[argi++]);

    int s = socket(AF_INET, SOCK_DGRAM, 0);
    if (s < 0) die("socket");

    // Increase socket buffers to tolerate loss/latency
    int bufsize = 16 * 1024 * 1024;
    setsockopt(s, SOL_SOCKET, SO_RCVBUF, &bufsize, sizeof(bufsize));
    setsockopt(s, SOL_SOCKET, SO_SNDBUF, &bufsize, sizeof(bufsize));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);
    if (bind(s, (sockaddr*)&addr, sizeof(addr)) < 0) die("bind");

    ServerState st{};
    st.sockfd = s;
    st.out_path = out_path;
    st.verbose = opt_verbose;

    recv_loop(st);

    // Compute and request retransmissions
    std::vector<uint32_t> missing;
    uint32_t total_missing = compute_missing(st, missing);
    unsigned round = 0;
    std::thread rx(recv_retrans, std::ref(st));

    while (total_missing > 0) {
        ++round;
        if (st.verbose) {
            std::cout << "NACK round " << round << ": missing " << total_missing << "\n";
        }
        bool high_loss = (st.chunk_count > 0) && (total_missing * 10 >= st.chunk_count); // >10%
        // Send NACK batches
        size_t sent = 0;
        while (sent < missing.size()) {
            size_t batch = std::min(static_cast<size_t>(MAX_NACK_IDS_PER_MSG), missing.size() - sent);
            size_t pkt_size = sizeof(NackMsgHdr) + batch * sizeof(uint32_t);
            std::vector<char> pkt(pkt_size);
            auto* h = reinterpret_cast<NackMsgHdr*>(pkt.data());
            std::memcpy(h->tag, "NACK", 4);
            h->count = static_cast<uint16_t>(batch);
            std::memcpy(pkt.data() + sizeof(NackMsgHdr), missing.data() + sent, batch * sizeof(uint32_t));

            if (sendto(st.sockfd, pkt.data(), pkt.size(), 0, (sockaddr*)&st.client, st.client_len) < 0) {
                die("sendto NACK");
            }
            // Light redundancy under high loss: repeat each batch once
            if (high_loss) {
                std::this_thread::sleep_for(std::chrono::microseconds(100));
                if (sendto(st.sockfd, pkt.data(), pkt.size(), 0, (sockaddr*)&st.client, st.client_len) < 0) {
                    die("sendto NACK dup");
                }
            }
            sent += batch;
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }

        // Allow time for retransmissions to arrive, then recompute
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        total_missing = compute_missing(st, missing);
    }

    st.receiving = false;
    rx.join();

    // Signal retransmissions complete
    send_ctrl(st, "DONE");

    // Expect FIN from client and reply FACK
    while (true) {
        CtrlMsg c{};
        ssize_t n = recvfrom(st.sockfd, &c, sizeof(c), 0, (sockaddr*)&st.client, &st.client_len);
        if (n < 0) die("recv FIN");
        if (n >= (ssize_t)sizeof(CtrlMsg) && tagEq(c.tag, "FIN ") && c.session_id == st.session_id) {
            send_ctrl(st, "FACK");
            break;
        }
    }

    if (st.out_fd >= 0) {
        ::fsync(st.out_fd);
        ::close(st.out_fd);
    }
    close(s);
    std::cout << "File transmission complete.\n";
    return 0;
}
