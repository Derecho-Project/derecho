#pragma once

#include <atomic>
#include <experimental/optional>
#include <iostream>
#include <map>
#include <memory>
#include <thread>
#include <vector>

#ifdef USE_VERBS_API
  #include "sst/verbs.h"
#else
  #include "sst/lf.h"
#endif

namespace sst {
struct P2PParams {
    uint32_t my_node_id;
    std::vector<uint32_t> members;
    uint32_t window_size;
    uint64_t max_p2p_size;
};

enum REQUEST_TYPE {
    P2P_QUERY,
    P2P_REPLY,
    P2P_SEND,
    RPC_REPLY
};

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
    std::vector<uint64_t> incoming_query_seq_nums, incoming_send_seq_nums, incoming_rpc_reply_seq_nums, incoming_p2p_reply_seq_nums,
            outgoing_query_seq_nums, outgoing_send_seq_nums, outgoing_rpc_reply_seq_nums, outgoing_p2p_reply_seq_nums;
    std::vector<REQUEST_TYPE> prev_mode;
    std::atomic<bool> thread_shutdown{false};
    std::thread timeout_thread;
    char* probe(uint32_t rank);
    uint32_t num_rdma_writes = 0;
    void check_failures_loop();

public:
    P2PConnections(const P2PParams params);
    P2PConnections(P2PConnections&& old_connections, const std::vector<uint32_t> new_members);
    ~P2PConnections();
    void shutdown_failures_thread();
    uint32_t get_node_rank(uint32_t node_id);
    uint64_t get_max_p2p_size();
    std::experimental::optional<std::pair<uint32_t, char*>> probe_all();
    char* get_sendbuffer_ptr(uint32_t rank, REQUEST_TYPE type);
    void send(uint32_t rank);
};
}  // namespace sst
