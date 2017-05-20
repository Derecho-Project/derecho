
#ifndef CONNECTION_H
#define CONNECTION_H

#include <functional>
#include <memory>
#include <string>

namespace tcp {

struct exception {};
struct connection_failure : public exception {};

class socket {
    int sock;

    explicit socket(int _sock) : sock(_sock), remote_ip() {}
    explicit socket(int _sock, std::string remote_ip)
            : sock(_sock), remote_ip(remote_ip) {}

    friend class connection_listener;

public:
    std::string remote_ip;

    socket() : sock(-1), remote_ip() {}
    socket(std::string servername, int port);
    socket(socket&& s);

    socket& operator=(socket& s) = delete;
    socket& operator=(socket&& s);

    ~socket();

    bool is_empty();
    std::string get_self_ip();

    bool read(char* buffer, size_t size);
    bool probe();
    bool write(char const* buffer, size_t size);

    template <class T>
    bool exchange(T local, T& remote) {
        static_assert(std::is_pod<T>::value,
                      "Can't send non-pod type over TCP");

        if(sock < 0) {
            fprintf(stderr, "WARNING: Attempted to write to closed socket\n");
            return false;
        }

        return write((char*)&local, sizeof(T)) &&
               read((char*)&remote, sizeof(T));
    }
};

class connection_listener {
    std::unique_ptr<int, std::function<void(int*)>> fd;

public:
    explicit connection_listener(int port);
    socket accept();
};
}

#endif /* CONNECTION_H */
