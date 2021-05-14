#pragma once

#include <atomic>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <thread>
#include <vector>
#include <mutex>
#include <functional>

#include "p2p_connection.hpp"
#ifdef USE_VERBS_API
#include <derecho/sst/detail/verbs.hpp>
#else
#include <derecho/sst/detail/lf.hpp>
#endif

namespace sst {

typedef std::function<void(node_id_t)> failure_upcall_t;

struct P2PParams {
    node_id_t my_node_id;
    uint32_t p2p_window_size;
    uint32_t rpc_window_size;
    uint64_t max_p2p_reply_size;
    uint64_t max_p2p_request_size;
    uint64_t max_rpc_reply_size;
    bool is_external;
    failure_upcall_t failure_upcall;
};

class P2PConnectionManager {
    const node_id_t my_node_id;

    RequestParams request_params;
    /**
     * Contains one entry per possible Node ID; the vector index is the node ID.
     * Each entry is a pair consisting of a mutex protecting that entry and a
     * possibly-null pointer to a P2PConnection to the node ID indicated by the
     * index. You must lock the mutex before accessing the pointer.
     */
    std::vector<std::pair<std::mutex, std::unique_ptr<P2PConnection>>> p2p_connections;
    /**
     * An array containing one Boolean value for each entry in p2p_connections
     * that serves as a hint for whether that entry is non-null. The values are
     * declared as char rather than bool to ensure they are each stored in
     * exactly one byte. This can be used to check whether a connection exists
     * without locking its mutex, but the hint can be wrong due to race
     * conditions. Threads that write to this array should lock the
     * corresponding mutex in p2p_connections before updating it to avoid lost
     * updates from write conflicts.
     */
    char* active_p2p_connections;

    uint64_t p2p_buf_size;
    std::atomic<bool> thread_shutdown{false};
    std::thread timeout_thread;

    void check_failures_loop();
    failure_upcall_t failure_upcall;
    std::mutex connections_mutex;

public:
    P2PConnectionManager(const P2PParams params);
    ~P2PConnectionManager();
    void shutdown_failures_thread();

    void add_connections(const std::vector<node_id_t>& node_ids);
    void remove_connections(const std::vector<node_id_t>& node_ids);
    bool contains_node(const node_id_t node_id);
    /**
     * @return the size of the byte array used for sending a single P2P reply
     * in any of the P2P connections. No messages larger than this can be sent.
     */
    std::size_t get_max_p2p_reply_size();
    /**
     * @return the size of the byte array used for sending a single RPC reply
     * in any of the P2P connections. No messages larger than this can be sent.
     */
    std::size_t get_max_rpc_reply_size();
    /**
     * Increments the sequence number of the last-received message type for
     * the specified node's connection.
     */
    void update_incoming_seq_num(node_id_t node_id);
    /**
     * Checks all the P2P connection buffers for new messages. If any
     * connection has a new message, this returns a pair containing the
     * sender's ID and a pointer into the message buffer.
     * @return (remote node ID, message byte buffer)
     */
    std::optional<std::pair<node_id_t, char*>> probe_all();
    /**
     * Returns a pointer to the beginning of the next available message buffer
     * for the specified request type in the specified node's P2P connection
     * channel, or a null pointer if no such message buffer is available.
     * @param node_id The ID of the remote node that will be sent to
     * @param type The type of P2P message to send
     * @return A pointer to the beginning of a message buffer, or null
     */
    char* get_sendbuffer_ptr(node_id_t node_id, REQUEST_TYPE type);
    /**
     * Sends the next outgoing message to the specified node, i.e. the one
     * populated by the most recent call to get_sendbuffer_ptr.
     * @param node_id The ID of the remote node to send to.
     */
    void send(node_id_t node_id);
    /**
     * Compares the set of P2P connections to a list of known live nodes and
     * removes any connections to nodes not in that list. This is used to
     * filter out connections to nodes that were removed from the view.
     * @param live_nodes_list A list of node IDs whose connections should be
     * retained; all other connections will be deleted.
     */
    void filter_to(const std::vector<node_id_t>& live_nodes_list);
    void debug_print();
};
}  // namespace sst
