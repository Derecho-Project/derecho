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
struct P2PParams {
    uint32_t my_node_id;
    std::vector<uint32_t> members;
    uint32_t window_size;
    uint64_t max_p2p_size;
};

enum REQUEST_TYPE {
    P2P_REPLY = 0,
    P2P_REQUEST,
    RPC_REPLY
};
static const REQUEST_TYPE p2p_request_types[] = {P2P_REPLY,
                                                 P2P_REQUEST,
                                                 RPC_REPLY};
static const uint8_t num_request_types = 3;

class P2PConnections {
    const std::vector<uint32_t> members;
    const std::uint32_t num_members;
    const uint32_t my_node_id;
    uint32_t my_index;
    const uint32_t window_size;
    const uint32_t max_msg_size;
    std::map<uint32_t, uint32_t> node_id_to_rank;
    // one element per member for P2P
    std::vector<std::unique_ptr<volatile char[]>> incoming_p2p_buffers;
    std::vector<std::unique_ptr<volatile char[]>> outgoing_p2p_buffers;
    std::vector<std::unique_ptr<resources>> res_vec;
    uint64_t p2p_buf_size;
    std::map<REQUEST_TYPE, std::vector<std::atomic<uint64_t>>> incoming_seq_nums_map, outgoing_seq_nums_map;
    std::vector<REQUEST_TYPE> prev_mode;
    std::atomic<bool> thread_shutdown{false};
    std::thread timeout_thread;
    uint64_t getOffsetSeqNum(REQUEST_TYPE type, uint64_t seq_num);
    uint64_t getOffsetBuf(REQUEST_TYPE type, uint64_t seq_num);
    char* probe(uint32_t rank);
    REQUEST_TYPE last_type;
    uint32_t last_rank;
    uint32_t num_rdma_writes = 0;
    void check_failures_loop();

public:
    P2PConnections(const P2PParams params);
    P2PConnections(P2PConnections&& old_connections, const std::vector<uint32_t> new_members);
    ~P2PConnections();
    void shutdown_failures_thread();
    uint32_t get_node_rank(uint32_t node_id);
    uint64_t get_max_p2p_size();
    std::optional<std::pair<uint32_t, char*>> probe_all();
    void update_incoming_seq_num();
    char* get_sendbuffer_ptr(uint32_t rank, REQUEST_TYPE type);
    void send(uint32_t rank);
    void debug_print();
};
}  // namespace sst
