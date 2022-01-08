#include "derecho/tcp/tcp.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <cassert>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <linux/tcp.h>
#include <netdb.h>
#include <sys/epoll.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <unistd.h>

namespace tcp {

socket::socket(std::string server_ip, uint16_t server_port, bool retry)
        : remote_port(server_port) {
    sock = ::socket(AF_INET, SOCK_STREAM, 0);
    if(sock < 0) throw connection_failure(server_ip);

    hostent* server;
    server = gethostbyname(server_ip.c_str());
    if(server == nullptr) throw connection_failure(server_ip);

    char server_ip_cstr[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, server->h_addr, server_ip_cstr, sizeof(server_ip_cstr));
    remote_ip = std::string(server_ip_cstr);

    sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(server_port);
    bcopy((char*)server->h_addr, (char*)&serv_addr.sin_addr.s_addr,
          server->h_length);

    int optval = 1;
    if(setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &optval, sizeof(optval))) {
        fprintf(stderr, "WARNING: Failed to disable Nagle's algorithm, continue without TCP_NODELAY...\n");
    }

    if(retry) {
        while(connect(sock, (sockaddr*)&serv_addr, sizeof(serv_addr)) < 0)
            ;
    } else {
        if(connect(sock, (sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
            throw connection_failure(server_ip);
        }
    }
}
socket::socket(socket&& s) : sock(s.sock), remote_ip(s.remote_ip), remote_port(s.remote_port) {
    s.sock = -1;
    s.remote_ip = std::string();
    s.remote_port = 0;
}

socket& socket::operator=(socket&& s) {
    sock = s.sock;
    s.sock = -1;
    remote_ip = std::move(s.remote_ip);
    remote_port = s.remote_port;
    s.remote_port = 0;
    return *this;
}

socket::~socket() {
    if(sock >= 0) close(sock);
}

bool socket::is_empty() const { return sock == -1; }

int socket::try_connect(std::string servername, uint16_t port, int timeout_ms) {
    sock = ::socket(AF_INET, SOCK_STREAM, 0);
    if(sock < 0) throw connection_failure(servername);

    hostent* server;
    server = gethostbyname(servername.c_str());
    if(server == nullptr) throw connection_failure(servername);

    char server_ip_cstr[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, server->h_addr, server_ip_cstr, sizeof(server_ip_cstr));
    remote_ip = std::string(server_ip_cstr);
    remote_port = port;

    sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    bcopy((char*)server->h_addr, (char*)&serv_addr.sin_addr.s_addr,
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
    int return_code = connect(sock, (sockaddr*)&serv_addr, sizeof(serv_addr));
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
        return_code = fcntl(sock, F_SETFL, sock_flags);  //This should return 0
    } else {
        close(sock);
    }
    close(epoll_fd);
    return return_code;
}

void socket::read(char* buffer, size_t size) {
    if(sock < 0) {
        throw socket_closed_error("Attempted to read from closed socket");
    }

    size_t total_bytes = 0;
    while(total_bytes < size) {
        ssize_t new_bytes = ::read(sock, buffer + total_bytes, size - total_bytes);
        if(new_bytes > 0) {
            total_bytes += new_bytes;
        } else if(new_bytes == 0) {
            throw incomplete_read_error("Read EOF prematurely");
        } else if(new_bytes == -1 && errno != EINTR) {
            throw socket_io_error(errno, "Read failed due to an error in socket connected to " + remote_ip);
        }
    }
}

ssize_t socket::read_partial(char* buffer, size_t max_size) {
    if(sock < 0) {
        throw socket_closed_error("Attempted to read from closed socket");
    }

    ssize_t bytes_read = ::read(sock, buffer, max_size);
    return bytes_read;
}
bool socket::probe() {
    int count;
    ioctl(sock, FIONREAD, &count);
    return count > 0;
}

void socket::write(const char* buffer, size_t size) {
    if(sock < 0) {
        throw socket_closed_error("Attempted to write to closed socket");
    }

    size_t total_bytes = 0;
    while(total_bytes < size) {
        //MSG_NOSIGNAL makes send return a proper error code if the socket has been
        //closed by the remote, rather than crashing the entire program with a SIGPIPE
        ssize_t bytes_written = send(sock, buffer + total_bytes, size - total_bytes, MSG_NOSIGNAL);
        if(bytes_written >= 0) {
            total_bytes += bytes_written;
        } else if(bytes_written == -1 && errno == ECONNRESET) {
            throw connection_reset_error("socket::write: Connection reset on socket to " + remote_ip);
        } else if(bytes_written == -1 && errno == EPIPE) {
            throw remote_closed_connection_error("socket::write: Socket closed by remote at " + remote_ip);
        } else if(bytes_written == -1 && errno != EINTR) {
            std::cerr << "socket::write: Error in the socket! Errno " << errno << std::endl;
            throw socket_io_error(errno, "socket::write: Unexpected error in socket connected to " + remote_ip);
        }
    }
}

std::string socket::get_self_ip() {
    struct sockaddr_storage my_addr_info;
    socklen_t len = sizeof my_addr_info;

    getsockname(sock, (struct sockaddr*)&my_addr_info, &len);
    char my_ip_cstr[INET6_ADDRSTRLEN + 1];
    if(my_addr_info.ss_family == AF_INET) {
        struct sockaddr_in* s = (struct sockaddr_in*)&my_addr_info;
        inet_ntop(AF_INET, &s->sin_addr, my_ip_cstr,
                  sizeof my_ip_cstr);
    } else {
        struct sockaddr_in6* s = (struct sockaddr_in6*)&my_addr_info;
        inet_ntop(AF_INET6, &s->sin6_addr, my_ip_cstr,
                  sizeof my_ip_cstr);
    }
    return std::string(my_ip_cstr);
}

connection_listener::connection_listener(uint16_t port, int queue_depth) : port(port) {
    sockaddr_in serv_addr;

    int listenfd = ::socket(AF_INET, SOCK_STREAM, 0);
    if(listenfd < 0) throw connection_failure("connection_listener: socket() failed");

    int reuse_addr = 1;
    setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, (char*)&reuse_addr,
               sizeof(reuse_addr));

    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(port);
    if(bind(listenfd, (sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        fprintf(stderr,
                "ERROR on binding to socket in ConnectionListener: %s\n",
                strerror(errno));
        std::cout << "Port is: " << port << std::endl;
        throw connection_failure("connection_listener: bind() failed");
    }
    if(listen(listenfd, queue_depth) < 0) {
        fprintf(stderr,
                "ERROR on listening to socket in ConnectionListener: %s\n",
                strerror(errno));
        throw connection_failure("connection_listener: listen() failed");
    }

    fd = std::unique_ptr<int, std::function<void(int*)>>(
            new int(listenfd), [](int* fd) { close(*fd); delete fd; });
}

socket connection_listener::accept() {
    char client_ip_cstr[INET6_ADDRSTRLEN + 1];
    struct sockaddr_storage client_addr_info;
    socklen_t len = sizeof client_addr_info;

    int sock = ::accept(*fd, (struct sockaddr*)&client_addr_info, &len);
    if(sock < 0) throw connection_failure("connection_listener: accept() failed");

    uint16_t client_port;
    if(client_addr_info.ss_family == AF_INET) {
        // Client has an IPv4 address
        struct sockaddr_in* s = (struct sockaddr_in*)&client_addr_info;
        inet_ntop(AF_INET, &s->sin_addr, client_ip_cstr, sizeof client_ip_cstr);
        client_port = ntohs(s->sin_port);
    } else {  // AF_INET6
        // Client has an IPv6 address
        struct sockaddr_in6* s = (struct sockaddr_in6*)&client_addr_info;
        inet_ntop(AF_INET6, &s->sin6_addr, client_ip_cstr,
                  sizeof client_ip_cstr);
        client_port = ntohs(s->sin6_port);
    }

    return socket(sock, std::string(client_ip_cstr), client_port);
}

std::optional<socket> connection_listener::try_accept(int timeout_ms) {
    //Temporarily set server socket to nonblocking
    int socket_flags = fcntl(*fd, F_GETFL, 0);
    socket_flags |= O_NONBLOCK;
    if(fcntl(*fd, F_SETFL, socket_flags) < 0) {
        return std::nullopt;
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
        client_sock = ::accept(*fd, (struct sockaddr*)&client_addr_info, &len);
        if(client_sock >= 0) {
            success = true;
        }
    }
    close(epoll_fd);

    //Set server socket back to blocking
    socket_flags = fcntl(*fd, F_GETFL, 0);
    socket_flags &= (~O_NONBLOCK);
    if(fcntl(*fd, F_SETFL, socket_flags) < 0) {
        return std::nullopt;
    }

    if(success) {
        char client_ip_cstr[INET6_ADDRSTRLEN + 1];
        uint16_t client_port;
        if(client_addr_info.ss_family == AF_INET) {
            // Client has an IPv4 address
            struct sockaddr_in* s = (struct sockaddr_in*)&client_addr_info;
            inet_ntop(AF_INET, &s->sin_addr, client_ip_cstr, sizeof client_ip_cstr);
            client_port = ntohs(s->sin_port);
        } else {  // AF_INET6
            // Client has an IPv6 address
            struct sockaddr_in6* s = (struct sockaddr_in6*)&client_addr_info;
            inet_ntop(AF_INET6, &s->sin6_addr, client_ip_cstr,
                      sizeof client_ip_cstr);
            client_port = ntohs(s->sin6_port);
        }
        return socket(client_sock, std::string(client_ip_cstr), client_port);
    } else {
        return std::nullopt;
    }
}

}  // namespace tcp
