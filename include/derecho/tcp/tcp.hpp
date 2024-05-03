#pragma once

#include "derecho/config.h"

#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>

namespace tcp {

/**
 * An exception that reports that a socket failed to connect and thus could not
 * be constructed.
 */
struct connection_failure : public std::runtime_error {
    connection_failure(const std::string& message = "") : runtime_error(message){};
};

/**
 * An exception that indicates that some kind of communication failure occurred
 * during a socket operation.
 */
struct socket_error : public std::runtime_error {
    socket_error(const std::string& message = "") : runtime_error(message){};
};

/**
 * An exception that reports that a socket operation failed because the socket
 * was closed.
 */
struct socket_closed_error : public socket_error {
    socket_closed_error(const std::string& message = "") : socket_error(message){};
};

/**
 * An exception that reports that a socket operation failed because of an I/O
 * error reported by the underlying OS socket system. Contains the "errno"
 * value reported by the socket operation.
 */
struct socket_io_error : public socket_error {
    const int errno_value;
    socket_io_error(int errno_value, const std::string& message = "")
            : socket_error(message), errno_value(errno_value){};
};

/**
 * An exception that reports that a socket read failed because the socket was
 * closed before the expected number of bytes could be read.
 */
struct incomplete_read_error : public socket_error {
    incomplete_read_error(const std::string& message = "") : socket_error(message){};
};

/**
 * An exception that reports that a socket operation failed because of the error
 * "connection reset by peer." This is a subclass of socket_io_error with the
 * errno value ECONNRESET.
 */
struct connection_reset_error : public socket_io_error {
    connection_reset_error(const std::string& message = "") : socket_io_error(ECONNRESET, message){};
};

/**
 * An exception that reports that a socket operation failed because of the error
 * "the local end of a connection-oriented socket has been shut down" (which
 * happens when the remote end terminates the TCP session). This is a subclass
 * of socket_io_error with the errno value EPIPE.
 */
struct remote_closed_connection_error : public socket_io_error {
    remote_closed_connection_error(const std::string& message = "") : socket_io_error(EPIPE, message){};
};

class socket {
    int sock;

    explicit socket(int _sock) : sock(_sock), remote_ip(), remote_port(0) {}
    /** A special constructor for connection_listener that wraps an existing C socket with this class. */
    explicit socket(int _sock, std::string remote_ip, uint16_t remote_port)
            : sock(_sock), remote_ip(remote_ip), remote_port(remote_port) {}

    friend class connection_listener;
    std::string remote_ip;
    uint16_t remote_port;

public:
    /**
     * Constructs an empty, unconnected socket.
     */
    socket() : sock(-1), remote_ip(), remote_port(0) {}
    /**
     * Constructs a socket connected to the specified address and port
     * @param server_ip The IP address of the remote host, as a string
     * @param server_port The port to connect to on the remote host
     * @param retry Whether to keep retrying until the connection succeeds.
     * If false, throws connection_failure if the first attempt to connect
     * does not succeed.
     * @throw connection_failure if local socket construction or IP address
     * lookup fails, or if retry = false and there was an error connecting
     * to the remote host.
     */
    socket(std::string server_ip, uint16_t server_port, bool retry = true);
    socket(socket&& s);

    socket& operator=(const socket& s) = delete;
    socket& operator=(socket&& s);

    ~socket();
    /**
     * @return True if the socket is empty (not connected), false otherwise
     */
    bool is_empty() const noexcept;
    /**
     * @return The local IP address this socket is bound to.
     * @throw socket_io_error if getsockname() fails.
     */
    std::string get_self_ip() const;
    /**
     * @return The IP address of the remote peer that this socket is connected
     * to (or an empty string if this socket is unconnected).
     */
    std::string get_remote_ip() const noexcept { return remote_ip; }
    /**
     * @return The TCP port on the remote peer that this socket is connected to
     * (or 0 if this socket is unconnected).
     */
    uint16_t get_remote_port() const noexcept { return remote_port; }

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
    int try_connect(std::string servername, uint16_t port, int timeout_ms = 20000);

    /**
     * Reads size bytes from the socket and writes them to the given buffer.
     * @param buffer A pointer to a byte buffer that should be used to store
     * the result of the read.
     * @param size The number of bytes to read.
     * @throw a subclass of socket_error if there was an error before all size
     * bytes could be read. The type of exception indicates the type of error:
     * socket_closed_error means the socket cannot be read from because it is
     * closed, incomplete_read_error means the connection was terminated (i.e.
     * read returned EOF) before all size bytes could be read, and
     * socket_io_error means some other error occurred during the read() call.
     */
    void read(uint8_t* buffer, size_t size);

    /**
     * Attempts to read up to max_size bytes from socket and write them to the
     * given buffer, but returns immediately even if fewer than max_size bytes
     * are available to be read. A very thin wrapper around a single read()
     * system call.
     * @param buffer A pointer to a byte buffer that should be used to store
     * the result of the read
     * @param max_size The number of bytes to attempt to read
     * @return The number of bytes actually read, or -1 if there was an error
     * @throw socket_closed_error if the socket is closed.
     */
    ssize_t read_partial(uint8_t* buffer, size_t max_size);

    /** Returns true if there is any data available to be read from the socket. */
    bool probe() const noexcept;

    /**
     * Writes size bytes from the given buffer to the socket.
     * @param buffer A pointer to a byte buffer whose data should be sent over
     * the socket.
     * @param size The number of bytes from the buffer to send.
     * @throw a subclass of socket_error if there was an error before all size
     * bytes could be written. The type of exception indicates the type of
     * error: socket_closed_error means the socket cannot be written to because
     * it is closed, while socket_io_error or one of its subclasses means an
     * error occurred during the write() call.
     */
    void write(const uint8_t* buffer, size_t size);

    /**
     * Convenience method for sending a single POD object (e.g. an int) over
     * the socket.
     * @param obj A POD object that can be sent by simply copying its memory
     * @throw A subclass of socket_error if there was an error while writing
     * to the socket.
     */
    template <typename T>
    void write(const T& obj) {
        static_assert(std::is_pod<T>::value, "Can't use simple socket::write on non-POD types");
        write(reinterpret_cast<const uint8_t*>(&obj), sizeof(obj));
    }

    /**
     * Convenience method for reading a single POD object from the socket and
     * writing it over a local value of that type. Hides the ugly cast to uint8_t*.
     * @param obj A local value of type T, which will be overwritten by a value
     * of the same size read from the socket.
     * @throw A subclass of socket_error if there was an error while attempting
     * to read from the socket.
     */
    template <typename T>
    void read(T& obj) {
        static_assert(std::is_pod<T>::value, "Can't use simple socket::read on non-POD types");
        read(reinterpret_cast<uint8_t*>(&obj), sizeof(obj));
    }

    /**
     * Convenience method that combines a read of a POD-type object with a write
     * of the same type. Used when exchanging ints or other simple values.
     * @tparam T The type of POD that will be read and written
     * @param local A local value of type T that will be sent over the socket
     * @param remote A reference to a value of type T that will be overwritten
     * by a read from the socket.
     * @throw A subclass of socket_error if there was an error during either the
     * read or the write
     */
    template <class T>
    void exchange(T local, T& remote) {
        static_assert(std::is_pod<T>::value, "Can't use socket::exchange on non-POD types");

        write(reinterpret_cast<uint8_t*>(&local), sizeof(T));
        read(reinterpret_cast<uint8_t*>(&remote), sizeof(T));
    }
};

class connection_listener {
    std::unique_ptr<int, std::function<void(int*)>> fd;
    uint16_t port;

public:
    /**
     * Constructs a connection listener ("server socket") that listens on the
     * given port of this machine's TCP interface.
     * @param port The port to listen on.
     * @param queue_depth The length of the pending connection queue to create
     * for the server socket. Defaults to 50.
     */
    explicit connection_listener(uint16_t port, int queue_depth = 50);

    /**
     * @return The local port that this connection listener is bound to.
     */
    uint16_t get_listening_port() const noexcept { return port; }

    /**
     * Blocks until a remote client makes a connection to this connection
     * listener, then returns a new socket connected to that client.
     * @return A socket connected to a remote client.
     * @throw connection_failure if the socket's accept operation fails
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
    std::optional<socket> try_accept(int timeout_ms) noexcept;
};
}  // namespace tcp
