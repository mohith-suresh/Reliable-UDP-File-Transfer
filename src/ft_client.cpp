// Early skeleton client: parse args and send a ping datagram
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <iostream>
#include <string>

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

    const char* msg = "hello";
    ssize_t n = sendto(s, msg, std::strlen(msg), 0, (sockaddr*)&dst, sizeof(dst));
    if (n < 0) die("sendto");
    std::cout << "Sent initial ping to server." << std::endl;
    close(s);
    return 0;
}
