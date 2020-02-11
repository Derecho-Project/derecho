#pragma once

#include <functional>
#include <memory>
#include <string>
#include <optional>

namespace tcp {

struct exception {};
struct connection_failure : public exception {};

class socket {
    int sock;

    explicit socket(int _sock) : sock(_sock), remote_ip() {}
    explicit socket(int _sock, std::string remote_ip)
            : sock(_sock), remote_ip(remote_ip) {}

    friend class connection_listener;
    std::string remote_ip;

public:

    /**
     * Constructs an empty, unconnected socket.
     */
    socket() : sock(-1), remote_ip() {}
    /**
     * Constructs a socket connected to the specified address and port,
     * blocking until the connection succeeds.
     * @param server_ip The IP address of the remote host, as a string
     * @param server_port The port to connect to on the remote host
     * @throws connection_failure if local socket construction or IP address
     * lookup fails.
     */
    socket(std::string server_ip, uint16_t server_port, bool retry=true);
    socket(socket&& s);

    socket& operator=(socket& s) = delete;
    socket& operator=(socket&& s);

    ~socket();

    bool is_empty() const;
    std::string get_self_ip();
    std::string get_remote_ip() const { return remote_ip; }

    /**
     * Attempts to connect the socket to the specified address and port, but
     * returns promptly with an error code if the connection attempt fails.
     * Also allows the caller to specify the timeout after which the connection
     * attempt will give up and return ETIMEDOUT.
     * @param servername The IP address of the remote host, as a string
     * @param port The port to connect to on the remote host
     * @param timeout_ms The number of milliseconds to wait for the remote host
     * to accept the connection; default is 20 seconds.
     * @return Zero if the connection was successful, or the error code (from
     * the set defined in sys/socket.h) that resulted from a failed connect()
     * system call.
     */
    int try_connect(std::string servername, int port, int timeout_ms = 20000);

    /**
     * Reads size bytes from the socket and writes them to the given buffer.
     * @param buffer A pointer to a byte buffer that should be used to store
     * the result of the read.
     * @param size The number of bytes to read.
     * @return True if the read was successful, false if there was an error
     * before size bytes could be read.
     */
    bool read(char* buffer, size_t size);

    /**
     * Attempts to read up to max_size bytes from socket and write them to the
     * given buffer, but returns immediately even if fewer than max_size bytes
     * are available to be read. A very thin wrapper around a single read()
     * system call.
     * @param buffer A pointer to a byte buffer that should be used to store
     * the result of the read
     * @param max_size The number of bytes to attempt to read
     * @return The number of bytes actually read, or -1 if there was an error
     */
    ssize_t read_partial(char* buffer, size_t max_size);

    /** Returns true if there is any data available to be read from the socket. */
    bool probe();

    /**
     * Writes size bytes from the given buffer to the socket.
     * @param buffer A pointer to a byte buffer whose data should be sent over
     * the socket.
     * @param size The number of bytes from the buffer to send.
     * @return True if the write was successful, false if there was an error
     * before size bytes could be written.
     */
    bool write(const char* buffer, size_t size);

    /**
     * Convenience method for sending a single POD object (e.g. an int) over
     * the socket.
     */
    template <typename T>
    bool write(const T& obj) {
        return write(reinterpret_cast<const char*>(&obj), sizeof(obj));
    }

    /**
     * Convenience method for reading a single POD object from the socket and
     * writing it over a local value of that type. Hides the ugly cast to char*.
     * @param obj A local value of type T, which will be overwritten by a value
     * of the same size read from the socket.
     */
    template <typename T>
    bool read(T& obj) {
        return read(reinterpret_cast<char*>(&obj), sizeof(obj));
    }

    template <class T>
    bool exchange(T local, T& remote) {
        static_assert(std::is_pod<T>::value,
                      "Can't send non-pod type over TCP");

        if(sock < 0) {
            fprintf(stderr, "WARNING: Attempted to write to closed socket\n");
            return false;
        }

        return write((char*)&local, sizeof(T)) && read((char*)&remote, sizeof(T));
    }
};

class connection_listener {
    std::unique_ptr<int, std::function<void(int*)>> fd;

public:
    /**
     * Constructs a connection listener ("server socket") that listens on the
     * given port of this machine's TCP interface.
     * @param port The port to listen on.
     */
    explicit connection_listener(uint16_t port);
    /**
     * Blocks until a remote client makes a connection to this connection
     * listener, then returns a new socket connected to that client.
     * @return A socket connected to a remote client.
     */
    socket accept();

    /**
     * Waits the specified number of milliseconds for a remote client to
     * connect to this connection listener, then either returns a new socket
     * connected to the client, or nullopt if no client connected before
     * the timeout.
     * @param timeout_ms The time to wait for a new connection
     * @return A socket connected to a remote client, or nullopt if the
     * timeout expired
     */
    std::optional<socket> try_accept(int timeout_ms);
};
}  // namespace tcp

