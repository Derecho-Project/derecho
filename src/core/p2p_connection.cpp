#include "derecho/core/detail/p2p_connection.hpp"

#include "derecho/conf/conf.hpp"
#include "derecho/core/detail/rpc_utils.hpp"
#include "derecho/sst/detail/poll_utils.hpp"

#include <cstring>
#include <map>
#include <sstream>
#include <sys/time.h>

namespace sst {

std::ostream& operator<<(std::ostream& os, MESSAGE_TYPE mt) {
    switch(mt) {
        case MESSAGE_TYPE::P2P_REPLY:
            os << "P2P reply";
            break;
        case MESSAGE_TYPE::P2P_REQUEST:
            os << "P2P request";
            break;
        case MESSAGE_TYPE::RPC_REPLY:
            os << "RPC reply";
            break;
    }
    return os;
}

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
    dbg_default_trace("P2PConnection updating incoming_seq_num for type {} to {}", type, incoming_seq_nums_map[type] + 1);
    incoming_seq_nums_map[type]++;
}

uint8_t* P2PConnection::get_sendbuffer_ptr(MESSAGE_TYPE type) {
    if(type != MESSAGE_TYPE::P2P_REQUEST
       || outgoing_seq_nums_map[MESSAGE_TYPE::P2P_REQUEST] - incoming_seq_nums_map[MESSAGE_TYPE::P2P_REPLY]
                  < connection_params.window_sizes[P2P_REQUEST]) {
        // C-style cast: reinterpret the bytes of the buffer as a uint64_t, and also cast away volatile
        ((uint64_t&)outgoing_p2p_buffer[getOffsetSeqNum(type, outgoing_seq_nums_map[type])])
                = outgoing_seq_nums_map[type] + 1;
        return const_cast<uint8_t*>(outgoing_p2p_buffer.get())
               + getOffsetBuf(type, outgoing_seq_nums_map[type]);
    }
    dbg_default_trace("P2PConnection: Send buffer was full: incoming_seq_nums[REPLY] = {}, but outgoing_seq_nums[REQUEST] = {}", incoming_seq_nums_map[MESSAGE_TYPE::P2P_REPLY], outgoing_seq_nums_map[MESSAGE_TYPE::P2P_REQUEST]);
    return nullptr;
}

void P2PConnection::send(MESSAGE_TYPE type) {
    if(remote_id == my_node_id) {
        // there's no reason why memcpy shouldn't also copy guard and data separately
        std::memcpy(const_cast<uint8_t*>(incoming_p2p_buffer.get()) + getOffsetBuf(type, outgoing_seq_nums_map[type]),
                    const_cast<uint8_t*>(outgoing_p2p_buffer.get()) + getOffsetBuf(type, outgoing_seq_nums_map[type]),
                    connection_params.max_msg_sizes[type] - sizeof(uint64_t));
        std::memcpy(const_cast<uint8_t*>(incoming_p2p_buffer.get()) + getOffsetSeqNum(type, outgoing_seq_nums_map[type]),
                    const_cast<uint8_t*>(outgoing_p2p_buffer.get()) + getOffsetSeqNum(type, outgoing_seq_nums_map[type]),
                    sizeof(uint64_t));
    } else {
        dbg_default_trace("Sending {} to node {}, about to call post_remote_write. getOffsetBuf() is {}, getOffsetSeqNum() is {}",
                          type, remote_id, getOffsetBuf(type, outgoing_seq_nums_map[type]), getOffsetSeqNum(type, outgoing_seq_nums_map[type]));
        uint64_t seq_num = ((uint64_t*)(outgoing_p2p_buffer.get() + getOffsetSeqNum(type, outgoing_seq_nums_map[type])))[0];
        long invocation_id = ((long*)(outgoing_p2p_buffer.get() + getOffsetBuf(type, outgoing_seq_nums_map[type]) + derecho::rpc::remote_invocation_utilities::header_space() + 1))[0];
        dbg_default_trace("Sequence number in the OffsetSeqNum position is {}. Invocation ID in the payload is {}", seq_num, invocation_id);
        res->post_remote_write(getOffsetBuf(type, outgoing_seq_nums_map[type]),
                               connection_params.max_msg_sizes[type] - sizeof(uint64_t));
        res->post_remote_write(getOffsetSeqNum(type, outgoing_seq_nums_map[type]),
                               sizeof(uint64_t));
    }
    outgoing_seq_nums_map[type]++;
}

P2PConnection::~P2PConnection() {}

}  // namespace sst
