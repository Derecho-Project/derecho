#include "derecho/core/detail/p2p_connection.hpp"

#include "derecho/conf/conf.hpp"
#include "derecho/core/detail/rpc_utils.hpp"
#include "derecho/sst/detail/poll_utils.hpp"

#include <cstring>
#include <map>
#include <sstream>
#include <sys/time.h>

namespace sst {
P2PConnection::P2PConnection(uint32_t my_node_id, uint32_t remote_id, uint64_t p2p_buf_size, const ConnectionParams& connection_params)
        : my_node_id(my_node_id), remote_id(remote_id), connection_params(connection_params) {
    incoming_p2p_buffer = std::make_unique<volatile uint8_t[]>(p2p_buf_size);
    outgoing_p2p_buffer = std::make_unique<volatile uint8_t[]>(p2p_buf_size);

    for(auto type : p2p_message_types) {
        incoming_seq_nums_map.try_emplace(type, 0);
        outgoing_seq_nums_map.try_emplace(type, 0);
    }

    if(my_node_id != remote_id) {
#ifdef USE_VERBS_API
        res = std::make_unique<resources>(remote_id, const_cast<uint8_t*>(incoming_p2p_buffer.get()),
                                          const_cast<uint8_t*>(outgoing_p2p_buffer.get()),
                                          p2p_buf_size, p2p_buf_size);
#else
        res = std::make_unique<resources>(remote_id, const_cast<uint8_t*>(incoming_p2p_buffer.get()),
                                          const_cast<uint8_t*>(outgoing_p2p_buffer.get()),
                                          p2p_buf_size, p2p_buf_size, my_node_id > remote_id);
#endif
    }
}

resources* P2PConnection::get_res() {
    return res.get();
}
uint64_t P2PConnection::getOffsetSeqNum(MESSAGE_TYPE type, uint64_t seq_num) {
    return connection_params.offsets[type] + connection_params.max_msg_sizes[type] * ((seq_num % connection_params.window_sizes[type]) + 1) - sizeof(uint64_t);
}

uint64_t P2PConnection::getOffsetBuf(MESSAGE_TYPE type, uint64_t seq_num) {
    return connection_params.offsets[type] + connection_params.max_msg_sizes[type] * (seq_num % connection_params.window_sizes[type]);
}

// check if there's a new request from some node
std::optional<std::pair<uint8_t*, MESSAGE_TYPE>> P2PConnection::probe() {
    for(auto type : p2p_message_types) {
        // C-style cast: reinterpret the bytes of the buffer as a uint64_t, and also cast away volatile
        if(((uint64_t&)incoming_p2p_buffer[getOffsetSeqNum(type, incoming_seq_nums_map[type])])
           == incoming_seq_nums_map[type] + 1) {
            return std::make_pair(const_cast<uint8_t*>(incoming_p2p_buffer.get())
                                          + getOffsetBuf(type, incoming_seq_nums_map[type]),
                                  type);
        }
    }
    return std::nullopt;
}

void P2PConnection::increment_incoming_seq_num(MESSAGE_TYPE type) {
    incoming_seq_nums_map[type]++;
}

std::optional<P2PBufferHandle> P2PConnection::get_sendbuffer_ptr(MESSAGE_TYPE type) {
    // For P2P_REQUEST buffers, check to ensure a buffer is available in the sending window by
    // comparing request and reply sequence numbers. P2P_REPLY and RPC_REPLY buffers are always
    // available, since they are only used in response to a message in the current sending window.
    if(type != MESSAGE_TYPE::P2P_REQUEST
       || outgoing_seq_nums_map[MESSAGE_TYPE::P2P_REQUEST] - incoming_seq_nums_map[MESSAGE_TYPE::P2P_REPLY]
                  < connection_params.window_sizes[P2P_REQUEST]) {
        uint64_t cur_seq_num = outgoing_seq_nums_map[type];
        uint64_t next_seq_num = ++outgoing_seq_nums_map[type];
        // C-style cast: reinterpret the bytes of the buffer as a uint64_t, and also cast away volatile
        ((uint64_t&)outgoing_p2p_buffer[getOffsetSeqNum(type, cur_seq_num)]) = next_seq_num;
        return P2PBufferHandle{const_cast<uint8_t*>(outgoing_p2p_buffer.get())
                                       + getOffsetBuf(type, cur_seq_num),
                               cur_seq_num};
    }
    return std::nullopt;
}

void P2PConnection::send(MESSAGE_TYPE type, uint64_t sequence_num) {
    if(remote_id == my_node_id) {
        // there's no reason why memcpy shouldn't also copy guard and data separately
        std::memcpy(const_cast<uint8_t*>(incoming_p2p_buffer.get()) + getOffsetBuf(type, sequence_num),
                    const_cast<uint8_t*>(outgoing_p2p_buffer.get()) + getOffsetBuf(type, sequence_num),
                    connection_params.max_msg_sizes[type] - sizeof(uint64_t));
        std::memcpy(const_cast<uint8_t*>(incoming_p2p_buffer.get()) + getOffsetSeqNum(type, sequence_num),
                    const_cast<uint8_t*>(outgoing_p2p_buffer.get()) + getOffsetSeqNum(type, sequence_num),
                    sizeof(uint64_t));
    } else {
        res->post_remote_write(getOffsetBuf(type, sequence_num),
                               connection_params.max_msg_sizes[type] - sizeof(uint64_t));
        res->post_remote_write(getOffsetSeqNum(type, sequence_num),
                               sizeof(uint64_t));
    }
}

P2PConnection::~P2PConnection() {}

}  // namespace sst
