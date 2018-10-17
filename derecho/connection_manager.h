#pragma once

#include <cassert>
#include <map>
#include <mutex>

#include "locked_reference.h"
#include "tcp/tcp.h"

namespace tcp {
using ip_addr_t = std::string;
using node_id_t = uint32_t;
class tcp_connections {
    std::mutex sockets_mutex;

    node_id_t my_id;
    const uint32_t port;
    std::unique_ptr<connection_listener> conn_listener;
    std::map<node_id_t, socket> sockets;
    bool add_connection(const node_id_t other_id,
                        const ip_addr_t& other_ip);
    void establish_node_connections(const std::map<node_id_t, ip_addr_t>& ip_addrs);

public:
    /**
     * Creates a TCP connection manager for a set of connections on the
     * specified port, and sets up connections to all of the initial set of
     * IP addresses.
     * @param _my_id The ID of this node
     * @param ip_addrs The list of IP addresses to connect to, indexed by node ID
     * @param _port The port to use for all TCP connections
     */
    tcp_connections(node_id_t _my_id,
                    const std::map<node_id_t, ip_addr_t>& ip_addrs,
                    uint32_t _port);
    void destroy();
    /**
     * Writes size bytes from a buffer to the node with ID node_id, using the
     * TCP socket connected to that node. Blocks until all the bytes have been
     * written or there is an error while writing.
     * @param node_id The ID of the node to send data to
     * @param buffer A byte buffer containing the data to send
     * @param size The number of bytes to send; must be <= the size of buffer
     * @return True if all bytes were written successfully, false if there was
     * an error.
     */
    bool write(node_id_t node_id, char const* buffer, size_t size);
    /**
     * Writes size bytes from a buffer to all the other nodes currently
     * connected, in ascending order of node ID.
     * @param buffer A byte buffer containing the data to send
     * @param size The number of bytes to send
     * @return True if all writes completed successfully, false if any of them
     * didn't.
     */
    bool write_all(char const* buffer, size_t size);
    /**
     * Receives size bytes from the node with ID node_id, over the TCP socket
     * connected to that node. Blocks until all the bytes have been received or
     * there is an error while reading.
     * @param node_id The ID of the node to read from
     * @param buffer A byte buffer to put received data into
     * @param size The number of bytes to read
     * @return True if all the bytes were read successfully, false if there was
     * an error.
     */
    bool read(node_id_t node_id, char* buffer, size_t size);
    /**
     * Adds a TCP connection to a new node. If the new node's ID is lower than
     * this node's ID, this function initiates a new TCP connection to it;
     * otherwise, this function listens on a TCP socket and waits for the new
     * node to make a connection.
     * @param new_id The ID of the new node
     * @param new_ip_addr The IP address of the new node
     * @return True if the TCP connection was set up successfully, false if
     * there was an error.
     */
    bool add_node(node_id_t new_id, const ip_addr_t new_ip_addr);
    /**
     * Removes a node from the managed set of TCP connections, closing the
     * socket connected to it.
     * @param remove_id The ID of the node to remove
     * @return True if the node/socket was removed successfully, false if it
     * was not.
     */
    bool delete_node(node_id_t remove_id);

    template <class T>
    bool exchange(node_id_t node_id, T local, T& remote) {
        std::lock_guard<std::mutex> lock(sockets_mutex);
        const auto it = sockets.find(node_id);
        assert(it != sockets.end());
        return it->second.exchange(local, remote);
    }
    /**
     * Checks all of the TCP connections managed by this object for new
     * incoming data, and returns the ID of the lowest-numbered node that has
     * data available to read. Returns -1 if none of the connected nodes have
     * any data ready to read.
     * @return The lowest node ID with data available in its TCP socket, or -1
     * if no sockets are ready to read.
     */
    int32_t probe_all();
    /**
     * Gets a locked reference to the TCP socket connected to a particular node.
     * While the caller holds the locked reference to the socket, no other
     * tcp_connections methods can be called. This makes it safe to use this
     * method to access sockets directly, even though they are usually managed
     * by the other tcp_connections methods.
     * @param node_id The ID of the desired node
     * @return A LockedReference to the TCP socket connected to that node.
     */
    derecho::LockedReference<std::unique_lock<std::mutex>, socket> get_socket(node_id_t node_id);
};
}  // namespace tcp
