#include <cassert>
#include <cstring>
#include <sstream>
#include <sys/time.h>

#include <derecho/conf/conf.hpp>
#include <derecho/core/detail/p2p_connection_manager.hpp>
#include <derecho/sst/detail/poll_utils.hpp>

namespace sst {
P2PConnectionManager::P2PConnectionManager(const P2PParams params)
        : my_node_id(params.my_node_id) {

    // HARD-CODED. Adding another request type will break this

    request_params.window_sizes[P2P_REPLY] = params.p2p_window_size;
    request_params.window_sizes[P2P_REQUEST] = params.p2p_window_size;
    request_params.window_sizes[RPC_REPLY] = params.rpc_window_size;
    request_params.max_msg_sizes[P2P_REPLY] = params.max_p2p_reply_size;
    request_params.max_msg_sizes[P2P_REQUEST] = params.max_p2p_request_size;
    request_params.max_msg_sizes[RPC_REPLY] = params.max_rpc_reply_size;

    p2p_buf_size = 0;
    for(uint8_t i = 0; i < num_request_types; ++i) {
        request_params.offsets[i] = p2p_buf_size;
        p2p_buf_size += request_params.window_sizes[i] * request_params.max_msg_sizes[i];
    }
    p2p_buf_size += sizeof(bool);

    p2p_connections[my_node_id] = std::make_unique<P2PConnection>(my_node_id, my_node_id, p2p_buf_size, request_params);

    timeout_thread = std::thread(&P2PConnectionManager::check_failures_loop, this);
}

P2PConnectionManager::~P2PConnectionManager() {
    shutdown_failures_thread();
}

void P2PConnectionManager::add_connections(const std::vector<uint32_t>& node_ids) {
    std::lock_guard<std::mutex> lock(connections_mutex);
    for (const uint32_t remote_id : node_ids) {
        p2p_connections.at(remote_id) = std::make_unique<P2PConnection>(my_node_id, remote_id, p2p_buf_size, request_params);
    }
}

void P2PConnectionManager::remove_connections(const std::vector<uint32_t>& node_ids) {
    std::lock_guard<std::mutex> lock(connections_mutex);
    for(const uint32_t remote_id : node_ids) {
        p2p_connections.erase(remote_id);
    }
}

void P2PConnectionManager::shutdown_failures_thread() {
    thread_shutdown = true;
    if(timeout_thread.joinable()) {
        timeout_thread.join();
    }
}

uint64_t P2PConnectionManager::get_max_p2p_reply_size() {
    return request_params.max_msg_sizes[P2P_REPLY] - sizeof(uint64_t);
}

void P2PConnectionManager::update_incoming_seq_num() {
    p2p_connections[last_node_id]->update_incoming_seq_num();
}

// check if there's a new request from any node
std::optional<std::pair<uint32_t, char*>> P2PConnectionManager::probe_all() {
    for(const auto& [node_id, p2p_conn] : p2p_connections) {
        auto buf = p2p_conn->probe();
        if(buf && buf[0]) {
            last_node_id = node_id;
            return std::pair<uint32_t, char*>(node_id, buf);
        } else if(buf) {
            // this means that we have a null reply
            // we don't need to process it, but we still want to increment the seq num
            p2p_conn->update_incoming_seq_num();
        }
    }
    return {};
}

char* P2PConnectionManager::get_sendbuffer_ptr(uint32_t node_id, REQUEST_TYPE type) {
    return p2p_connections.at(node_id)->get_sendbuffer_ptr(type);
}

void P2PConnectionManager::send(uint32_t node_id) {
    p2p_connections.at(node_id)->send();
    if(node_id != my_node_id) {
        num_rdma_writes++;
    }
}

void P2PConnectionManager::check_failures_loop() {
    pthread_setname_np(pthread_self(), "p2p_timeout");

    uint32_t heartbeat_ms = derecho::getConfUInt32(CONF_DERECHO_HEARTBEAT_MS);
    const auto tid = std::this_thread::get_id();
    // get id first
    uint32_t ce_idx = util::polling_data.get_index(tid);
    while(!thread_shutdown) {
        std::this_thread::sleep_for(std::chrono::milliseconds(heartbeat_ms));
        if(num_rdma_writes < 1000) {
            continue;
        }
        num_rdma_writes = 0;

        util::polling_data.set_waiting(tid);
#ifdef USE_VERBS_API
        std::map<uint32_t, struct verbs_sender_ctxt> sctxt;
#else
        std::map<uint32_t, struct lf_sender_ctxt> sctxt;
#endif

        for(const auto& [node_id, p2p_conn] : p2p_connections) {
            if (node_id == my_node_id) {
                continue;
            }
            sctxt[node_id].remote_id = node_id;
            sctxt[node_id].ce_idx = ce_idx;

            p2p_conn->get_res()->post_remote_write_with_completion(&sctxt[node_id], p2p_buf_size - sizeof(bool), sizeof(bool));
        } 
        /** Completion Queue poll timeout in millisec */
        const int MAX_POLL_CQ_TIMEOUT = 2000;
        unsigned long start_time_msec;
        unsigned long cur_time_msec;
        struct timeval cur_time;

        // wait for completion for a while before giving up of doing it ..
        gettimeofday(&cur_time, NULL);
        start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);

        uint32_t num_completions = 0;
        while(num_completions < p2p_connections.size() - 1) {
            std::optional<std::pair<int32_t, int32_t>> ce;
            while(true) {
                // check if polling result is available
                ce = util::polling_data.get_completion_entry(tid);
                if(ce) {
                    break;
                }
                gettimeofday(&cur_time, NULL);
                cur_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
                if((cur_time_msec - start_time_msec) >= MAX_POLL_CQ_TIMEOUT) {
                    break;
                }
            }
            if(!ce) {
                break;
            }
            num_completions++;
        }
        util::polling_data.reset_waiting(tid);
    }
}

void P2PConnectionManager::debug_print() {
    // std::cout << "Members: " << std::endl;
    // for(const auto& [node_id, p2p_conn] : p2p_connections) {
    //     std::cout << node_id << " ";
    // }
    // std::cout << std::endl;

    // for(const auto& type : p2p_request_types) {
    //     std::cout << "P2PConnections: Request type " << type << std::endl;
    //     for(uint32_t node = 0; node < num_members; ++node) {
    //         std::cout << "Node " << node << std::endl;
    //         std::cout << "incoming seq_nums:";
    //         for(uint32_t i = 0; i < request_params.window_sizes[type]; ++i) {
    //             uint64_t offset = request_params.max_msg_sizes[type] * (type * request_params.window_sizes[type] + i + 1) - sizeof(uint64_t);
    //             std::cout << " " << (uint64_t&)p2p_connections[node]->incoming_p2p_buffer[offset];
    //         }
    //         std::cout << std::endl
    //                   << "outgoing seq_nums:";
    //         for(uint32_t i = 0; i < request_params.window_sizes[type]; ++i) {
    //             uint64_t offset = request_params.max_msg_sizes[type] * (type * request_params.window_sizes[type] + i + 1) - sizeof(uint64_t);
    //             std::cout << " " << (uint64_t&)p2p_connections[node]->outgoing_p2p_buffer[offset];
    //         }
    //         std::cout << std::endl;
    //     }
    // }
}
}  // namespace sst
