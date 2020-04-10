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
    // one element per member for P2P
    std::map<node_id_t, std::unique_ptr<P2PConnection>> p2p_connections;

    uint64_t p2p_buf_size;
    std::atomic<bool> thread_shutdown{false};
    std::thread timeout_thread;
    
    node_id_t last_node_id;
    void check_failures_loop();
    failure_upcall_t failure_upcall;
    std::mutex connections_mutex;

public:
    P2PConnectionManager(const P2PParams params);
    ~P2PConnectionManager();
    void add_connections(const std::vector<node_id_t>& node_ids);
    void remove_connections(const std::vector<node_id_t>& node_ids);
    bool contains_node(const node_id_t node_id);
    void shutdown_failures_thread();
    uint64_t get_max_p2p_reply_size();
    void update_incoming_seq_num();
    std::optional<std::pair<node_id_t, char*>> probe_all();
    char* get_sendbuffer_ptr(node_id_t node_id, REQUEST_TYPE type);
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
