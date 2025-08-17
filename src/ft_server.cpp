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
    // Minimal wait loop (no protocol yet)
    char buf[2048];
    sockaddr_in cli{}; socklen_t cli_len = sizeof(cli);
    ssize_t n = recvfrom(s, buf, sizeof(buf), 0, (sockaddr*)&cli, &cli_len);
    if (n >= 0) {
        std::cout << "Received initial datagram (" << n << " bytes).\n";
    }
    close(s);
    return 0;
}
