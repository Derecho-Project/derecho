#include "derecho/core/detail/p2p_connection_manager.hpp"

#include "derecho/core/derecho_exception.hpp"
#include "derecho/conf/conf.hpp"
#include "derecho/sst/detail/poll_utils.hpp"
#include "derecho/utils/logger.hpp"

#include <cassert>
#include <cstring>
#include <mutex>
#include <optional>
#include <sstream>
#include <sys/time.h>
#include <thread>
#include <unordered_set>

namespace sst {
P2PConnectionManager::P2PConnectionManager(const P2PParams params)
        : my_node_id(params.my_node_id),
          rpc_logger(spdlog::get(LoggerFactory::RPC_LOGGER_NAME)),
          p2p_connections(derecho::getConfUInt32(CONF_DERECHO_MAX_NODE_ID)),
          active_p2p_connections(new char[derecho::getConfUInt32(CONF_DERECHO_MAX_NODE_ID)]),
          failure_upcall(params.failure_upcall) {
    // HARD-CODED. Adding another request type will break this

    request_params.window_sizes[P2P_REPLY] = params.p2p_window_size;
    request_params.window_sizes[P2P_REQUEST] = params.p2p_window_size;
    request_params.window_sizes[RPC_REPLY] = params.rpc_window_size;
    request_params.max_msg_sizes[P2P_REPLY] = params.max_p2p_reply_size;
    request_params.max_msg_sizes[P2P_REQUEST] = params.max_p2p_request_size;
    request_params.max_msg_sizes[RPC_REPLY] = params.max_rpc_reply_size;

    for(uint32_t i = 0; i < derecho::getConfUInt32(CONF_DERECHO_MAX_NODE_ID); ++i) {
        active_p2p_connections[i] = false;
    }

    p2p_buf_size = 0;
    for(uint8_t i = 0; i < num_p2p_message_types; ++i) {
        request_params.offsets[i] = p2p_buf_size;
        p2p_buf_size += request_params.window_sizes[i] * request_params.max_msg_sizes[i];
    }
    p2p_buf_size += sizeof(bool);

    p2p_connections[my_node_id].second = std::make_unique<P2PConnection>(my_node_id, my_node_id, p2p_buf_size, request_params);
    active_p2p_connections[my_node_id] = true;

    // external client doesn't need failure checking
    if(!params.is_external) {
        timeout_thread = std::thread(&P2PConnectionManager::check_failures_loop, this);
    }
}

P2PConnectionManager::~P2PConnectionManager() {
    shutdown_failures_thread();
    //plain C array must be deleted
    delete[] active_p2p_connections;
}

void P2PConnectionManager::add_connections(const std::vector<node_id_t>& node_ids) {
    for(const node_id_t remote_id : node_ids) {
        std::lock_guard<std::mutex> connection_lock(p2p_connections[remote_id].first);
        if(!p2p_connections[remote_id].second) {
            p2p_connections[remote_id].second = std::make_unique<P2PConnection>(my_node_id, remote_id, p2p_buf_size, request_params);
            active_p2p_connections[remote_id] = true;
        }
    }
}

void P2PConnectionManager::remove_connections(const std::vector<node_id_t>& node_ids) {
    for(const node_id_t remote_id : node_ids) {
        std::lock_guard<std::mutex> connection_lock(p2p_connections[remote_id].first);
        p2p_connections[remote_id].second = nullptr;
        active_p2p_connections[remote_id] = false;
    }
}

bool P2PConnectionManager::contains_node(const node_id_t node_id) {
    std::lock_guard<std::mutex> lock(p2p_connections[node_id].first);
    return p2p_connections[node_id].second != nullptr;
}

void P2PConnectionManager::shutdown_failures_thread() {
    thread_shutdown = true;
    if(timeout_thread.joinable()) {
        timeout_thread.join();
    }
}

std::size_t P2PConnectionManager::get_max_p2p_reply_size() {
    return request_params.max_msg_sizes[P2P_REPLY] - sizeof(uint64_t);
}

std::size_t P2PConnectionManager::get_max_rpc_reply_size() {
    return request_params.max_msg_sizes[RPC_REPLY] - sizeof(uint64_t);
}

void P2PConnectionManager::increment_incoming_seq_num(node_id_t node_id, MESSAGE_TYPE type) {
    if(node_id != INVALID_NODE_ID) {
        std::lock_guard<std::mutex> connection_lock(p2p_connections[node_id].first);
        if(p2p_connections[node_id].second) {
            p2p_connections[node_id].second->increment_incoming_seq_num(type);
        }
    }
}

// check if there's a new request from any node
std::optional<MessagePointer> P2PConnectionManager::probe_all() {
    for(node_id_t node_id = 0; node_id < p2p_connections.size(); ++node_id) {
        //Check the hint before locking the mutex. If it's false, don't bother.
        if(!active_p2p_connections[node_id]) continue;

        std::lock_guard<std::mutex> connection_lock(p2p_connections[node_id].first);
        //In case the hint was wrong, check for an empty connection
        if(!p2p_connections[node_id].second) continue;

        auto buf_type_pair = p2p_connections[node_id].second->probe();
        // In include/derecho/core/detail/rpc_utils.hpp:
        // Please note that populate_header() put payload_size(size_t) at the beginning of buffer.
        // If we only test buf[0], it will fall in the wrong path if the least significant byte of the payload size is
        // zero.
        if(buf_type_pair && reinterpret_cast<size_t*>(buf_type_pair->first)[0] != 0) {
            return MessagePointer{node_id, buf_type_pair->first, buf_type_pair->second};
        } else if(buf_type_pair) {
            // this means that we have a null reply
            // we don't need to process it, but we still want to increment the seq num
            dbg_trace(rpc_logger, "Got a null reply from node {} for a void P2P call", node_id);
            p2p_connections[node_id].second->increment_incoming_seq_num(buf_type_pair->second);
            return MessagePointer{INVALID_NODE_ID, nullptr, MESSAGE_TYPE::P2P_REPLY};
        }
    }
    return {};
}

std::optional<P2PBufferHandle> P2PConnectionManager::get_sendbuffer_ptr(node_id_t node_id, MESSAGE_TYPE type) {
    std::lock_guard<std::mutex> connection_lock(p2p_connections[node_id].first);
    if(p2p_connections[node_id].second) {
        return p2p_connections[node_id].second->get_sendbuffer_ptr(type);
    }

    // Weijia: we should report an exception instead of just return a nullptr because a connection to node_id does not exists.
    throw std::out_of_range(std::string(__PRETTY_FUNCTION__) + " cannot find a connection to node:" + std::to_string(node_id));
}

void P2PConnectionManager::send(node_id_t node_id, MESSAGE_TYPE type, uint64_t sequence_num) {
    std::lock_guard<std::mutex> connection_lock(p2p_connections[node_id].first);
    p2p_connections[node_id].second->send(type, sequence_num);
    if(node_id != my_node_id && p2p_connections[node_id].second) {
        p2p_connections[node_id].second->num_rdma_writes++;
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
    const uint16_t one_second_count = 1000 / heartbeat_ms;
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

        for(node_id_t node_id = 0; node_id < p2p_connections.size(); ++node_id) {
            if(!active_p2p_connections[node_id]) continue;

            std::lock_guard<std::mutex> connection_lock(p2p_connections[node_id].first);

            if(!p2p_connections[node_id].second) continue;

            // checks every second regardless of num_rdma_writes
            if(node_id == my_node_id || (p2p_connections[node_id].second->num_rdma_writes < 1000 && tick_count < one_second_count)) {
                continue;
            }
            p2p_connections[node_id].second->num_rdma_writes = 0;
            sctxt[node_id].set_remote_id(node_id);
            sctxt[node_id].set_ce_idx(ce_idx);

            p2p_connections[node_id].second->get_res()->post_remote_write_with_completion(&sctxt[node_id],
                                                                                          p2p_buf_size - sizeof(bool),
                                                                                          sizeof(bool));
            posted_write_to.insert(node_id);
        }
        if(tick_count >= one_second_count) {
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
                for(const node_id_t& posted_id : posted_write_to) {
                    if(polled_successfully_from.find(posted_id) != polled_successfully_from.end()) {
                        continue;
                    }
                    failed_node_indexes.push_back(posted_id);
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
            {
                std::lock_guard<std::mutex> connection_lock(p2p_connections[nid].first);
                if(p2p_connections[nid].second) {
                    p2p_connections[nid].second->get_res()->report_failure();
                    //Note for the future: It would be nice to add the failed node ID to a
                    //thread-local "already failed" list, so that this thread doesn't re-check
                    //it between now and the next view change.
                }
            }
            //Release lock before calling the upcall, since it may call remove_connections
            if(failure_upcall) {
                failure_upcall(nid);
            }
        }
    }
}

void P2PConnectionManager::filter_to(const std::vector<node_id_t>& live_nodes_list) {
    std::vector<node_id_t> prev_nodes_list;
    for(node_id_t node_id = 0; node_id < p2p_connections.size(); ++node_id) {
        //Check the hint before acquiring the lock
        if(!active_p2p_connections[node_id]) continue;

        std::lock_guard<std::mutex> connection_lock(p2p_connections[node_id].first);
        //Check again to ensure the connection is non-null
        if(p2p_connections[node_id].second) {
            prev_nodes_list.push_back(node_id);
        }
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

    // for(const auto& type : p2p_message_types) {
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

void P2PConnectionManager::oob_remote_write(const node_id_t& remote_node, const struct iovec* iov, int iovcnt, uint64_t remote_dest_addr, uint64_t rkey, size_t size) {
    std::lock_guard lck(p2p_connections[remote_node].first);
    if (p2p_connections[remote_node].second == nullptr) {
        throw derecho::derecho_exception("oob write to unconnected node:" + std::to_string(remote_node));
    }
    if (active_p2p_connections[remote_node] == false) {
        throw derecho::derecho_exception("oob write to inactive node:" + std::to_string(remote_node));
    }
    p2p_connections[remote_node].second->oob_remote_write(iov,iovcnt,reinterpret_cast<void*>(remote_dest_addr),rkey,size);
}

void P2PConnectionManager::oob_remote_read(const node_id_t& remote_node, const struct iovec* iov, int iovcnt, uint64_t remote_src_addr, uint64_t rkey, size_t size) {
    std::lock_guard lck(p2p_connections[remote_node].first);
    if (p2p_connections[remote_node].second == nullptr) {
        throw derecho::derecho_exception("oob read from unconnected node:" + std::to_string(remote_node));
    }
    if (active_p2p_connections[remote_node] == false) {
        throw derecho::derecho_exception("oob read from inactive node:" + std::to_string(remote_node));
    }
    p2p_connections[remote_node].second->oob_remote_read(iov,iovcnt,reinterpret_cast<void*>(remote_src_addr),rkey,size);
}

}  // namespace sst
