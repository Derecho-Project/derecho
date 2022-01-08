#pragma once


#include "../derecho_type_definitions.hpp"
#include "locked_reference.hpp"
#include "derecho/tcp/tcp.hpp"

#include <cassert>
#include <map>
#include <mutex>

namespace tcp {
class tcp_connections {
    std::mutex sockets_mutex;

    node_id_t my_id;
    std::unique_ptr<connection_listener> conn_listener;
    std::map<node_id_t, socket> sockets;
    bool add_connection(const node_id_t other_id,
                        const std::pair<ip_addr_t, uint16_t>& other_ip_and_port);
    void establish_node_connections(
            const std::map<node_id_t, std::pair<ip_addr_t, uint16_t>>& ip_addrs_and_ports);

public:
    /**
     * Creates a TCP connection manager for a set of connections
     * to all of the initial set of addresses.
     * @param my_id The ID of this node
     * @param ip_addrs_and_ports The map of IP address-port pairs to connect to, indexed by node ID
     */
    tcp_connections(node_id_t my_id,
                    const std::map<node_id_t, std::pair<ip_addr_t, uint16_t>> ip_addrs_and_ports);
    void destroy();
    /**
     * Writes size bytes from a buffer to the node with ID node_id, using the
     * TCP socket connected to that node. Blocks until all the bytes have been
     * written or there is an error while writing.
     * @param node_id The ID of the node to send data to
     * @param buffer A byte buffer containing the data to send
     * @param size The number of bytes to send; must be <= the size of buffer
     * @throw a subclass of socket_error if there was an error before all size
     * bytes could be written. The type of exception indicates the type of
     * error: socket_closed_error means the socket cannot be written to because
     * it is closed, while socket_io_error or one of its subclasses means an
     * error occurred during the write() call.
     */
    void write(node_id_t node_id, char const* buffer, size_t size);
    /**
     * Writes size bytes from a buffer to all the other nodes currently
     * connected, in ascending order of node ID.
     * @param buffer A byte buffer containing the data to send
     * @param size The number of bytes to send
     * @throw a subclass of socket_error if there was an error before all size
     * bytes could be written on all sockets.
     */
    void write_all(char const* buffer, size_t size);
    /**
     * Receives size bytes from the node with ID node_id, over the TCP socket
     * connected to that node. Blocks until all the bytes have been received or
     * there is an error while reading.
     * @param node_id The ID of the node to read from
     * @param buffer A byte buffer to put received data into
     * @param size The number of bytes to read
     * @throw a subclass of socket_error if there was an error before all size
     * bytes could be read. The type of exception indicates the type of error:
     * socket_closed_error means the socket cannot be read from because it is
     * closed, incomplete_read_error means the connection was terminated (i.e.
     * read returned EOF) before all size bytes could be read, and
     * socket_io_error means some other error occurred during the read() call.
     */
    void read(node_id_t node_id, char* buffer, size_t size);
    /**
     * Adds a TCP connection to a new node. If the new node's ID is lower than
     * this node's ID, this function initiates a new TCP connection to it;
     * otherwise, this function listens on a TCP socket and waits for the new
     * node to make a connection.
     * @param new_id The ID of the new node
     * @param new_ip_addr_and_port The IP address and port number of the new node
     * @return True if the TCP connection was set up successfully, false if
     * there was an error.
     */
    bool add_node(node_id_t new_id,
                  const std::pair<ip_addr_t, uint16_t>& new_ip_addr_and_port);
    /**
     * Removes a node from the managed set of TCP connections, closing the
     * socket connected to it.
     * @param remove_id The ID of the node to remove
     * @return True if the node/socket was removed successfully, false if it
     * was not.
     */
    bool delete_node(node_id_t remove_id);
    /**
     * Checks whether this connection manager currently has a socket connected
     * to the node with the specified ID
     * @param node_id The node ID to check
     * @return True if this connection manager has a socket for that node ID,
     * false if it does not.
     */
    bool contains_node(node_id_t node_id);

    template <class T>
    void exchange(node_id_t node_id, T local, T& remote) {
        std::lock_guard<std::mutex> lock(sockets_mutex);
        const auto it = sockets.find(node_id);
        assert(it != sockets.end());
        it->second.exchange(local, remote);
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
     * Compares the set of TCP connections to a list of known live nodes and
     * removes any connections to nodes not in that list. This is used to
     * filter out connections to nodes that were removed from the view.
     * @param live_nodes_list A list of node IDs whose connections should be
     * retained; all other connections will be deleted.
     */
    void filter_to(const std::vector<node_id_t>& live_nodes_list);

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
