#pragma once

#include <atomic>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <thread>
#include <vector>
#include <mutex>

#include "p2p_connection.hpp"
#ifdef USE_VERBS_API
#include <derecho/sst/detail/verbs.hpp>
#else
#include <derecho/sst/detail/lf.hpp>
#endif

namespace sst {

struct P2PParams {
    uint32_t my_node_id;
    uint32_t p2p_window_size;
    uint32_t rpc_window_size;
    uint64_t max_p2p_reply_size;
    uint64_t max_p2p_request_size;
    uint64_t max_rpc_reply_size;
};

class P2PConnectionManager {
    const uint32_t my_node_id;

    RequestParams request_params;
    // one element per member for P2P
    std::map<uint32_t, std::unique_ptr<P2PConnection>> p2p_connections;

    uint64_t p2p_buf_size;
    std::atomic<bool> thread_shutdown{false};
    std::thread timeout_thread;
    
    uint32_t last_node_id;
    uint32_t num_rdma_writes = 0;
    void check_failures_loop();

    std::mutex connections_mutex;

public:
    P2PConnectionManager(const P2PParams params);
    ~P2PConnectionManager();
    void add_connections(const std::vector<uint32_t>& node_ids);
    void remove_connections(const std::vector<uint32_t>& node_ids);
    void shutdown_failures_thread();
    uint64_t get_max_p2p_reply_size();
    void update_incoming_seq_num();
    std::optional<std::pair<uint32_t, char*>> probe_all();
    char* get_sendbuffer_ptr(uint32_t node_id, REQUEST_TYPE type);
    void send(uint32_t node_id);
    void debug_print();
};
}  // namespace sst
