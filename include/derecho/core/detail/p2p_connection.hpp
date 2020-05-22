#pragma once

#include <atomic>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <thread>
#include <vector>

#ifdef USE_VERBS_API
#include <derecho/sst/detail/verbs.hpp>
#else
#include <derecho/sst/detail/lf.hpp>
#endif

namespace sst {
class P2PConnectionManager;

enum REQUEST_TYPE {
    P2P_REPLY = 0,
    P2P_REQUEST,
    RPC_REPLY
};
static const REQUEST_TYPE p2p_request_types[] = {P2P_REPLY,
                                                 P2P_REQUEST,
                                                 RPC_REPLY};
static const uint8_t num_request_types = 3;

struct RequestParams {
    uint32_t window_sizes[num_request_types];
    uint32_t max_msg_sizes[num_request_types];
    uint64_t offsets[num_request_types];
};

class P2PConnection {
    const uint32_t my_node_id;
    const uint32_t remote_id;
    const RequestParams& request_params;
    std::unique_ptr<volatile char[]> incoming_p2p_buffer;
    std::unique_ptr<volatile char[]> outgoing_p2p_buffer;
    std::unique_ptr<resources> res;
    std::map<REQUEST_TYPE, std::atomic<uint64_t>> incoming_seq_nums_map, outgoing_seq_nums_map;
    REQUEST_TYPE prev_mode;
    REQUEST_TYPE last_type;
    uint64_t getOffsetSeqNum(REQUEST_TYPE type, uint64_t seq_num);
    uint64_t getOffsetBuf(REQUEST_TYPE type, uint64_t seq_num);
    
protected:
    friend class P2PConnectionManager;
    resources* get_res();
    uint32_t num_rdma_writes = 0;

public:
    P2PConnection(uint32_t my_node_id, uint32_t remote_id, uint64_t p2p_buf_size, const RequestParams& request_params);
    ~P2PConnection();
    
    char* probe();
    void update_incoming_seq_num();
    char* get_sendbuffer_ptr(REQUEST_TYPE type);
    void send();
    
};
}  // namespace sst
