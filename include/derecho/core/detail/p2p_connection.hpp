#pragma once

#ifdef USE_VERBS_API
#include "derecho/sst/detail/verbs.hpp"
#else
#include "derecho/sst/detail/lf.hpp"
#endif

#include <atomic>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <thread>
#include <vector>

namespace sst {
class P2PConnectionManager;

enum MESSAGE_TYPE {
    P2P_REPLY = 0,
    P2P_REQUEST,
    RPC_REPLY
};
static const MESSAGE_TYPE p2p_message_types[] = {P2P_REPLY,
                                                 P2P_REQUEST,
                                                 RPC_REPLY};
static const uint8_t num_p2p_message_types = 3;

struct ConnectionParams {
    uint32_t window_sizes[num_p2p_message_types];
    uint32_t max_msg_sizes[num_p2p_message_types];
    uint64_t offsets[num_p2p_message_types];
};

class P2PConnection {
    const uint32_t my_node_id;
    const uint32_t remote_id;
    const ConnectionParams& connection_params;
    std::unique_ptr<volatile uint8_t[]> incoming_p2p_buffer;
    std::unique_ptr<volatile uint8_t[]> outgoing_p2p_buffer;
    std::unique_ptr<resources> res;
    std::map<MESSAGE_TYPE, std::atomic<uint64_t>> incoming_seq_nums_map, outgoing_seq_nums_map;
    uint64_t getOffsetSeqNum(MESSAGE_TYPE type, uint64_t seq_num);
    uint64_t getOffsetBuf(MESSAGE_TYPE type, uint64_t seq_num);

protected:
    friend class P2PConnectionManager;
    resources* get_res();
    uint32_t num_rdma_writes = 0;

public:
    P2PConnection(uint32_t my_node_id, uint32_t remote_id, uint64_t p2p_buf_size,
                  const ConnectionParams& connection_params);
    ~P2PConnection();

    /**
     * Returns the pair (pointer into an incoming message buffer, type of message)
     * if there is a new incoming message from the remote node, or std::nullopt if
     * there are no new messages.
     */
    std::optional<std::pair<uint8_t*, MESSAGE_TYPE>> probe();
    /**
     * Increments the incoming sequence number for the specified message type,
     * indicating that the caller is finished handling the current incoming
     * message of that type.
     */
    void increment_incoming_seq_num(MESSAGE_TYPE type);
    /**
     * Returns a pointer to the beginning of the next available message buffer
     * for the specified message type, or a null pointer if no message buffer
     * is available.
     */
    uint8_t* get_sendbuffer_ptr(MESSAGE_TYPE type);
    /**
     * Sends the next outgoing message, i.e. the one populated by the most
     * recent call to get_sendbuffer_ptr, of the specified message type.
     */
    void send(MESSAGE_TYPE type);
};
}  // namespace sst
