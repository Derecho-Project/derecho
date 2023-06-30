#pragma once

#include "p2p_connection.hpp"
#ifdef USE_VERBS_API
#include "derecho/sst/detail/verbs.hpp"
#else
#include "derecho/sst/detail/lf.hpp"
#endif
#include "derecho/utils/logger.hpp"

#include <atomic>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

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

struct MessagePointer {
    node_id_t sender_id;
    uint8_t* buf;
    MESSAGE_TYPE type;
};

class P2PConnectionManager {
    const node_id_t my_node_id;
    /** A pointer to the RPC-module logger (since these P2P connections are used for Derecho RPC) */
    std::shared_ptr<spdlog::logger> rpc_logger;
    ConnectionParams request_params;
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
     * Increments the sequence number of the specified message type for
     * the specified node's connection, indicating that the caller is done
     * receiving the current message of that type.
     */
    void increment_incoming_seq_num(node_id_t node_id, MESSAGE_TYPE type);
    /**
     * Checks all the P2P connection buffers for new messages. If any
     * connection has a new message, this returns a MessagePointer object
     * describing the message: the sender's ID, a pointer into the message
     * buffer, and the type of message in the buffer.
     * @return A MessagePointer struct, or std::nullopt if no connection has a new message.
     */
    std::optional<MessagePointer> probe_all();
    /**
     * Returns a P2PBufferHandle for the next available message buffer
     * for the specified message type in the specified node's P2P connection
     * channel, or std::nullopt if no such message buffer is available.
     * @param node_id The ID of the remote node that will be sent to
     * @param type The type of P2P message to send
     * @return A P2PBufferHandle containing a pointer to the beginning
     * of a message buffer, or std::nullopt
     */
    std::optional<P2PBufferHandle> get_sendbuffer_ptr(node_id_t node_id, MESSAGE_TYPE type);
    /**
     * Sends a specific outgoing message to a node, assuming its buffer has been
     * populated by a previous call to get_sendbuffer_ptr(). The message buffer
     * identified by the message type and sequence number will be sent over the
     * P2P connection to that node.
     * @param node_id The ID of the remote node to send to.
     * @param type The type of message being sent
     * @param sequence_num The sequence number of the buffer to send.
     */
    void send(node_id_t node_id, MESSAGE_TYPE type, uint64_t sequence_num);
    /**
     * Compares the set of P2P connections to a list of known live nodes and
     * removes any connections to nodes not in that list. This is used to
     * filter out connections to nodes that were removed from the view.
     * @param live_nodes_list A list of node IDs whose connections should be
     * retained; all other connections will be deleted.
     */
    void filter_to(const std::vector<node_id_t>& live_nodes_list);
    void debug_print();
    /**
     * write to remote OOB memory
     * @param remote_node       remote node id
     * @param iov               gather of local memory regions
     * @param iovcnt
     * @param remote_dest_addr  the address of the remote memory region
     * @param rkey              the access key for remote memory
     * @param size              the size of the remote memory region
     *
     * @throw                   derecho::derecho_exception on error
     */
    void oob_remote_write(const node_id_t& remote_node, const struct iovec* iov, int iovcnt, uint64_t remote_dest_addr, uint64_t rkey, size_t size);
    /**
     * read from remote OOB memory
     * @param remote_node       remote node id
     * @param iov               scatter of local memory regions
     * @param iovcnt
     * @param remote_src_addr   the address of the remote memory region
     * @param rkey              the access key for remote memory
     * @param size              the size of the remote memory region
     *
     * @throw                   derecho::derecho_exception on error
     */
    void oob_remote_read(const node_id_t& remote_node, const struct iovec* iov, int iovcnt, uint64_t remote_srcaddr, uint64_t rkey, size_t size);

    /**
     * send to data in local buffer to remote
     * @param remote_node       remote node id
     * @param iov               scatter of local memory regions
     * @param iovcnt
     *
     * @throw                   derecho::derecho_exception on error
     */
    void oob_send(const node_id_t& remote_node, const struct iovec* iov, int iovcnt);

    /**
     * receive remote data to local buffer
     * @param remote_node       remote node id
     * @param iov               scatter of local memory regions
     * @param iovcnt
     *
     * @throw                   derecho::derecho_exception on error
     */
    void oob_recv(const node_id_t& remote_node, const struct iovec* iov, int iovcnt);

    /**
     * wait for non-blocking data
     * @param remote_node       remote node id
     * @param op                operation id
     *
     * @throw                   derecho::derecho_exception on error
     */
    void wait_for_oob_op(const node_id_t& remote_node, uint32_t op);
};
}  // namespace sst
