/*
 * connection_test.cpp
 *
 */

#include "tcp/tcp.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstdio>
#include <cstring>
#include <functional>
#include <iostream>

static constexpr int TEST_PORTNUM = 6666;

using std::string;
using std::unique_ptr;
using std::cout;
using std::endl;

int main(int argc, char **argv) {
    string serverIP;
    bool isServer;
    if(argc < 2) {
        isServer = true;
    } else {
        isServer = false;
        serverIP = argv[1];
    }

    if(isServer) {
        // Set up server socket - copied from connection.cpp
        sockaddr_in serv_addr;

        int port = TEST_PORTNUM;
        int listenfd = socket(AF_INET, SOCK_STREAM, 0);
        if(listenfd < 0) return -1;

        int reuse_addr = 1;
        setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, (char *)&reuse_addr,
                   sizeof(reuse_addr));

        memset(&serv_addr, 0, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_addr.s_addr = INADDR_ANY;
        serv_addr.sin_port = htons(port);
        if(bind(listenfd, (sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
            fprintf(stderr, "ERROR on binding\n");
        listen(listenfd, 5);
        std::unique_ptr<int, std::function<void(int *)>> fd;
        fd = unique_ptr<int, std::function<void(int *)>>(
            new int(listenfd), [](int *fd) { close(*fd); });

        // Accept client connection - copied from Stackoverflow
        int client_socket_fd;
        socklen_t len;
        struct sockaddr_storage client_addr_info;
        char client_ip_cstr[INET6_ADDRSTRLEN + 1];
        int client_port;

        len = sizeof client_addr_info;
        client_socket_fd =
            accept(*fd, (struct sockaddr *)&client_addr_info, &len);
        std::cout << "Accepted client connection. ";

        if(client_addr_info.ss_family == AF_INET) {
            // Client has an IPv4 address
            struct sockaddr_in *s = (struct sockaddr_in *)&client_addr_info;
            client_port = ntohs(s->sin_port);
            inet_ntop(AF_INET, &s->sin_addr, client_ip_cstr,
                      sizeof client_ip_cstr);
        } else {  // AF_INET6
            // Client has an IPv6 address
            struct sockaddr_in6 *s = (struct sockaddr_in6 *)&client_addr_info;
            client_port = ntohs(s->sin6_port);
            inet_ntop(AF_INET6, &s->sin6_addr, client_ip_cstr,
                      sizeof client_ip_cstr);
        }

        printf("Connected node's IP address: %s:%d\n", client_ip_cstr,
               client_port);

        close(client_socket_fd);

    } else {
        cout << "Connecting to " << serverIP << " on port " << TEST_PORTNUM
             << endl;
        tcp::socket clientSocket(serverIP, TEST_PORTNUM);
    }

    return 0;
}
