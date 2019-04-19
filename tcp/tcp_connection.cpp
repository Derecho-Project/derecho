#include "tcp_connection.h"

#include <algorithm>
#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <unistd.h>
#include <cassert>
#include <linux/tcp.h>

namespace tcp {

using namespace std;

socket::socket(string server_ip, uint16_t server_port) {
    sock = ::socket(AF_INET, SOCK_STREAM, 0);
    if(sock < 0) throw connection_failure();

    hostent *server;
    server = gethostbyname(server_ip.c_str());
    if(server == nullptr) throw connection_failure();

    char server_ip_cstr[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, server->h_addr, server_ip_cstr, sizeof(server_ip_cstr));
    remote_ip = string(server_ip_cstr);

    sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(server_port);
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr,
          server->h_length);

    int optval = 1;
    if(setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &optval, sizeof(optval))) {
      fprintf(stderr, "WARNING: Failed to disable Nagle's algorithm, continue without TCP_NODELAY...\n");
    }

    while(connect(sock, (sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
        /* do nothing*/;
}
socket::socket(socket &&s) : sock(s.sock), remote_ip(s.remote_ip) {
    s.sock = -1;
    s.remote_ip = ip_addr_t();
}

socket &socket::operator=(socket &&s) {
    sock = s.sock;
    s.sock = -1;
    remote_ip = std::move(s.remote_ip);
    return *this;
}

socket::~socket() {
    if(sock >= 0) close(sock);
}

bool socket::is_empty() const { return sock == -1; }

int socket::try_connect(string servername, int port, int timeout_ms) {
    sock = ::socket(AF_INET, SOCK_STREAM, 0);
    if(sock < 0) throw connection_failure();

    hostent *server;
    server = gethostbyname(servername.c_str());
    if(server == nullptr) throw connection_failure();

    char server_ip_cstr[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, server->h_addr, server_ip_cstr, sizeof(server_ip_cstr));
    remote_ip = string(server_ip_cstr);

    sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr,
          server->h_length);

    //Temporarily set socket to nonblocking in order to connect with a timeout
    int sock_flags = fcntl(sock, F_GETFL, 0);
    sock_flags |= O_NONBLOCK;
    fcntl(sock, F_SETFL, sock_flags);
    int epoll_fd = epoll_create1(0);
    struct epoll_event connect_event;
    memset(&connect_event, 0, sizeof(connect_event));
    connect_event.data.fd = sock;
    connect_event.events = EPOLLOUT;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, sock, &connect_event);

    bool connected = false;
    int return_code = connect(sock, (sockaddr *)&serv_addr, sizeof(serv_addr));
    if(return_code == -1 && errno != EINPROGRESS) {
        return_code = errno;
    } else if(return_code == 0) {
        //Successfully connected right away
        connected = true;
    } else {
        epoll_event array_of_one_event[1];
        //Wait for the EPOLLOUT event indicating the socket is connected
        int numfds = epoll_wait(epoll_fd, array_of_one_event, 1, timeout_ms);
        if(numfds == 0) {
            //Timed out
            return_code = ETIMEDOUT;
        } else if(numfds < 0) {
            return_code = errno;
        } else {
            assert(numfds == 1);
            assert(array_of_one_event[0].data.fd == sock);
            if(array_of_one_event[0].events & EPOLLERR) {
                return_code = 0;
                socklen_t len = sizeof return_code;
                getsockopt(sock, SOL_SOCKET, SO_ERROR, &return_code, &len);
            } else {
                assert(array_of_one_event[0].events & EPOLLOUT);
                connected = true;
            }
        }
    }
    if(connected) {
        //Connection was successful, set the socket back to blocking
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, sock, NULL);
        sock_flags = fcntl(sock, F_GETFL, 0);
        sock_flags &= ~O_NONBLOCK;
        return_code = fcntl(sock, F_SETFL, sock_flags); //This should return 0
    } else {
        close(sock);
    }
    close(epoll_fd);
    return return_code;
}

bool socket::read(char *buffer, size_t size) {
    if(sock < 0) {
        fprintf(stderr, "WARNING: Attempted to read from closed socket\n");
        return false;
    }

    size_t total_bytes = 0;
    while(total_bytes < size) {
        ssize_t new_bytes = ::read(sock, buffer + total_bytes, size - total_bytes);
        if(new_bytes > 0) {
            total_bytes += new_bytes;
        } else if(new_bytes == 0 || (new_bytes == -1 && errno != EINTR)) {
            return false;
        }
    }
    return true;
}

ssize_t socket::read_partial(char *buffer, size_t max_size) {
    if(sock < 0) {
        fprintf(stderr, "WARNING: Attempted to read from closed socket\n");
        return -1;
    }

    ssize_t bytes_read = ::read(sock, buffer, max_size);
    return bytes_read;
}
bool socket::probe() {
    int count;
    ioctl(sock, FIONREAD, &count);
    return count > 0;
}

bool socket::write(const char *buffer, size_t size) {
    if(sock < 0) {
        fprintf(stderr, "WARNING: Attempted to write to closed socket\n");
        return false;
    }

    size_t total_bytes = 0;
    while(total_bytes < size) {
        //MSG_NOSIGNAL makes send return a proper error code if the socket has been
        //closed by the remote, rather than crashing the entire program with a SIGPIPE
        ssize_t bytes_written = send(sock, buffer + total_bytes, size - total_bytes, MSG_NOSIGNAL);
        if(bytes_written >= 0) {
            total_bytes += bytes_written;
        } else if(bytes_written == -1 && errno != EINTR) {
            std::cerr << "socket::write: Error in the socket! Errno " << errno << std::endl;
            return false;
        }
    }
    return true;
}

ip_addr_t socket::get_self_ip() {
    struct sockaddr_storage my_addr_info;
    socklen_t len = sizeof my_addr_info;

    getsockname(sock, (struct sockaddr *)&my_addr_info, &len);
    char my_ip_cstr[INET6_ADDRSTRLEN + 1];
    if(my_addr_info.ss_family == AF_INET) {
        struct sockaddr_in *s = (struct sockaddr_in *)&my_addr_info;
        inet_ntop(AF_INET, &s->sin_addr, my_ip_cstr,
                  sizeof my_ip_cstr);
    } else {
        struct sockaddr_in6 *s = (struct sockaddr_in6 *)&my_addr_info;
        inet_ntop(AF_INET6, &s->sin6_addr, my_ip_cstr,
                  sizeof my_ip_cstr);
    }
    return ip_addr_t(my_ip_cstr);
}

connection_listener::connection_listener(uint16_t port) {
    sockaddr_in serv_addr;

    int listenfd = ::socket(AF_INET, SOCK_STREAM, 0);
    if(listenfd < 0) throw connection_failure();

    int reuse_addr = 1;
    setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, (char *)&reuse_addr,
               sizeof(reuse_addr));

    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(port);
    if(bind(listenfd, (sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        fprintf(stderr,
                "ERROR on binding to socket in ConnectionListener: %s\n",
                strerror(errno));
        std::cout << "Port is: " << port << std::endl;
    }
    listen(listenfd, 5);

    fd = unique_ptr<int, std::function<void(int *)>>(
            new int(listenfd), [](int *fd) { close(*fd); delete fd; });
}

socket connection_listener::accept() {
    char client_ip_cstr[INET6_ADDRSTRLEN + 1];
    struct sockaddr_storage client_addr_info;
    socklen_t len = sizeof client_addr_info;

    int sock = ::accept(*fd, (struct sockaddr *)&client_addr_info, &len);
    if(sock < 0) throw connection_failure();

    if(client_addr_info.ss_family == AF_INET) {
        // Client has an IPv4 address
        struct sockaddr_in *s = (struct sockaddr_in *)&client_addr_info;
        inet_ntop(AF_INET, &s->sin_addr, client_ip_cstr, sizeof client_ip_cstr);
    } else {  // AF_INET6
        // Client has an IPv6 address
        struct sockaddr_in6 *s = (struct sockaddr_in6 *)&client_addr_info;
        inet_ntop(AF_INET6, &s->sin6_addr, client_ip_cstr,
                  sizeof client_ip_cstr);
    }

    return socket(sock, ip_addr_t(client_ip_cstr));
}

std::optional<socket> connection_listener::try_accept(int timeout_ms) {
    //Temporarily set server socket to nonblocking
    int socket_flags = fcntl(*fd, F_GETFL, 0);
    socket_flags |= O_NONBLOCK;
    if(fcntl(*fd, F_SETFL, socket_flags) < 0) {
        throw connection_failure();
    }

    int epoll_fd = epoll_create1(0);
    struct epoll_event accept_event;
    memset(&accept_event, 0, sizeof(accept_event));
    accept_event.data.fd = *fd;
    accept_event.events = EPOLLIN;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, *fd, &accept_event);

    struct sockaddr_storage client_addr_info;
    socklen_t len = sizeof client_addr_info;
    int client_sock;
    bool success = false;
    epoll_event array_of_one_event[1];
    //Wait for the EPOLLIN event indicating the socket is connected
    int numfds = epoll_wait(epoll_fd, array_of_one_event, 1, timeout_ms);
    if(numfds == 1) {
        assert(array_of_one_event[0].data.fd == *fd);
        client_sock = ::accept(*fd, (struct sockaddr *)&client_addr_info, &len);
        if(client_sock >= 0) {
            success = true;
        }
    }
    close(epoll_fd);

    //Set server socket back to blocking
    socket_flags = fcntl(*fd, F_GETFL, 0);
    socket_flags &= (~O_NONBLOCK);
    if(fcntl(*fd, F_SETFL, socket_flags) < 0) {
        throw connection_failure();
    }

    if(success) {
        char client_ip_cstr[INET6_ADDRSTRLEN + 1];
        if(client_addr_info.ss_family == AF_INET) {
        // Client has an IPv4 address
        struct sockaddr_in *s = (struct sockaddr_in *)&client_addr_info;
        inet_ntop(AF_INET, &s->sin_addr, client_ip_cstr, sizeof client_ip_cstr);
        } else {  // AF_INET6
            // Client has an IPv6 address
            struct sockaddr_in6 *s = (struct sockaddr_in6 *)&client_addr_info;
            inet_ntop(AF_INET6, &s->sin6_addr, client_ip_cstr,
                      sizeof client_ip_cstr);
        }
        return socket(client_sock, ip_addr_t(client_ip_cstr));
    } else {
        return std::nullopt;
    }

}

}  // namespace tcp
