#include <cassert>
#include <cstring>
#include <sstream>
#include <sys/time.h>
#include <unordered_set>

#include <derecho/conf/conf.hpp>
#include <derecho/core/detail/p2p_connection_manager.hpp>
#include <derecho/sst/detail/poll_utils.hpp>
#include <derecho/utils/logger.hpp>
namespace sst {
P2PConnectionManager::P2PConnectionManager(const P2PParams params)
        : my_node_id(params.my_node_id),
          failure_upcall(params.failure_upcall) {
    
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

    // external client doesn't need failure checking
    if (!params.is_external) {
        timeout_thread = std::thread(&P2PConnectionManager::check_failures_loop, this);
    }
}

P2PConnectionManager::~P2PConnectionManager() {
    shutdown_failures_thread();
}

void P2PConnectionManager::add_connections(const std::vector<node_id_t>& node_ids) {
    std::lock_guard<std::mutex> lock(connections_mutex);
    for (const node_id_t remote_id : node_ids) {
	if (p2p_connections.find(remote_id) == p2p_connections.end()) {
	    p2p_connections.emplace(remote_id, std::make_unique<P2PConnection>(my_node_id, remote_id, p2p_buf_size, request_params));
    	}
    }
}

void P2PConnectionManager::remove_connections(const std::vector<node_id_t>& node_ids) {
    std::lock_guard<std::mutex> lock(connections_mutex);
    for(const node_id_t remote_id : node_ids) {
        p2p_connections.erase(remote_id);
    }
}

bool P2PConnectionManager::contains_node(const node_id_t node_id) {
    std::lock_guard<std::mutex> lock(connections_mutex);
    return (p2p_connections.find(node_id) != p2p_connections.end());
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
std::optional<std::pair<node_id_t, char*>> P2PConnectionManager::probe_all() {
    for(const auto& [node_id, p2p_conn] : p2p_connections) {
        auto buf = p2p_conn->probe();
        if(buf && buf[0]) {
            last_node_id = node_id;
            return std::pair<node_id_t, char*>(node_id, buf);
        } else if(buf) {
            // this means that we have a null reply
            // we don't need to process it, but we still want to increment the seq num
            p2p_conn->update_incoming_seq_num();
            return std::pair<node_id_t, char*>(INVALID_NODE_ID, nullptr);
        }
    }
    return {};
}

char* P2PConnectionManager::get_sendbuffer_ptr(node_id_t node_id, REQUEST_TYPE type) {
    return p2p_connections.at(node_id)->get_sendbuffer_ptr(type);
}

void P2PConnectionManager::send(node_id_t node_id) {
    p2p_connections.at(node_id)->send();
    if(node_id != my_node_id) {
        p2p_connections.at(node_id)->num_rdma_writes++;
    }
}

void P2PConnectionManager::check_failures_loop() {
    pthread_setname_np(pthread_self(), "p2p_timeout");

    // using CONF_DERECHO_HEARTBEAT_MS from derecho.cfg
    uint32_t heartbeat_ms = derecho::getConfUInt32(CONF_DERECHO_HEARTBEAT_MS);
    const auto tid = std::this_thread::get_id();
    // get id first
    uint32_t ce_idx = util::polling_data.get_index(tid);

    uint16_t tick_count = 0;
    const uint16_t one_second_count = 1000/heartbeat_ms;
    while(!thread_shutdown) {
        std::this_thread::sleep_for(std::chrono::milliseconds(heartbeat_ms));
        tick_count++;
        std::unordered_set<node_id_t> posted_write_to;

        util::polling_data.set_waiting(tid);
#ifdef USE_VERBS_API
        std::map<uint32_t, verbs_sender_ctxt> sctxt;
#else
        std::map<uint32_t, lf_sender_ctxt> sctxt;
#endif

        for(const auto& [node_id, p2p_conn] : p2p_connections) {
            // checks every second regardless of num_rdma_writes
            if (node_id == my_node_id || (p2p_conn->num_rdma_writes < 1000 && tick_count < one_second_count)) {
                continue;
            }
            p2p_conn->num_rdma_writes = 0;
            sctxt[node_id].set_remote_id(node_id);
            sctxt[node_id].set_ce_idx(ce_idx);

            p2p_conn->get_res()->post_remote_write_with_completion(&sctxt[node_id], p2p_buf_size - sizeof(bool), sizeof(bool));
            posted_write_to.insert(node_id);
        } 
        if (tick_count >= one_second_count) {
            tick_count = 0;
        }

        // track which nodes respond successfully
        std::unordered_set<node_id_t> polled_successfully_from;
        std::vector<node_id_t> failed_node_indexes;

        /** Completion Queue poll timeout in millisec */
        const unsigned int MAX_POLL_CQ_TIMEOUT = derecho::getConfUInt32(CONF_DERECHO_SST_POLL_CQ_TIMEOUT_MS);
        unsigned long start_time_msec;
        unsigned long cur_time_msec;
        struct timeval cur_time;

        // wait for completion for a while before giving up of doing it ..
        gettimeofday(&cur_time, NULL);
        start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);

        for(unsigned int i = 0; i < posted_write_to.size(); i++) {
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
                    tick_count += MAX_POLL_CQ_TIMEOUT;
                    break;
                }
            }
            // if waiting for a completion entry timed out
            if(!ce) {
                // mark all nodes that have not yet responded as failed
                for(const auto& pair : p2p_connections) {
                    const auto& node_id = pair.first;
                    if(posted_write_to.find(node_id) == posted_write_to.end() 
                    || polled_successfully_from.find(node_id) != polled_successfully_from.end()) {
                        continue;
                    }
                    failed_node_indexes.push_back(node_id);
                }
                break;
            }

            auto ce_v = ce.value();
            int remote_id = ce_v.first;
            int result = ce_v.second;
            if(result == 1) {
                polled_successfully_from.insert(remote_id);
            } else if(result == -1) {
                failed_node_indexes.push_back(remote_id);
            }
        }
        util::polling_data.reset_waiting(tid);

        for(auto nid : failed_node_indexes) {
            dbg_default_debug("p2p_connection_manager detected failure/timeout on node {}", nid);
            p2p_connections.at(nid)->get_res()->report_failure();
            if(failure_upcall) {
                failure_upcall(nid);
            }
        }
    }
}


void P2PConnectionManager::filter_to(const std::vector<node_id_t>& live_nodes_list) {
    std::vector<node_id_t> prev_nodes_list;
    for (const auto& e : p2p_connections ) {
        prev_nodes_list.push_back(e.first);
    }
    std::vector<node_id_t> departed;
    std::set_difference(prev_nodes_list.begin(), prev_nodes_list.end(),
                        live_nodes_list.begin(), live_nodes_list.end(),
                        std::back_inserter(departed));
    remove_connections(departed);
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
