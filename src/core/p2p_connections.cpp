#include <map>

#include <cassert>
#include <cstring>
#include <sstream>
#include <sys/time.h>

#include <derecho/conf/conf.hpp>
#include <derecho/core/detail/p2p_connections.hpp>
#include <derecho/sst/detail/poll_utils.hpp>

namespace sst {
P2PConnections::P2PConnections(const P2PParams params)
        : members(params.members),
          num_members(members.size()),
          my_node_id(params.my_node_id),
          incoming_p2p_buffers(num_members),
          outgoing_p2p_buffers(num_members),
          res_vec(num_members),
          prev_mode(num_members) {
    //Figure out my SST index
    my_index = (uint32_t)-1;
    for(uint32_t i = 0; i < num_members; ++i) {
        if(members[i] == my_node_id) {
            my_index = i;
        }
        node_id_to_rank[members[i]] = i;
        node_id_to_ip_addr[members[i]] = params.ip_addr[i];
    }

    // HARD-CODED. Adding another request type will break this
    window_sizes[P2P_REPLY] = params.p2p_window_size;
    window_sizes[P2P_REQUEST] = params.p2p_window_size;
    window_sizes[RPC_REPLY] = params.rpc_window_size;
    max_msg_sizes[P2P_REPLY] = params.max_p2p_reply_size;
    max_msg_sizes[P2P_REQUEST] = params.max_p2p_request_size;
    max_msg_sizes[RPC_REPLY] = params.max_rpc_reply_size;

    p2p_buf_size = 0;
    for(uint8_t i = 0; i < num_request_types; ++i) {
        offsets[i] = p2p_buf_size;
        p2p_buf_size += window_sizes[i] * max_msg_sizes[i];
    }
    p2p_buf_size += sizeof(bool);

    for(auto type : p2p_request_types) {
        incoming_seq_nums_map.try_emplace(type, std::vector<std::atomic<uint64_t>>(num_members));
        outgoing_seq_nums_map.try_emplace(type, std::vector<std::atomic<uint64_t>>(num_members));
    }

    timeout_thread = std::thread(&P2PConnections::check_failures_loop, this);
    tcp_connections_thread = std::thread(&P2PConnections::check_tcp_connections, this);
    tcp_connections_thread.detach();
    // std::cout << "TCP Conn started" << std::endl;
}

P2PConnections::P2PConnections(P2PConnections&& old_connections, const std::vector<uint32_t> new_members, const std::vector<ip_addr_t> ip_addr_new_members)
        : members(new_members),
          num_members(members.size()),
          my_node_id(old_connections.my_node_id),
          incoming_p2p_buffers(num_members),
          outgoing_p2p_buffers(num_members),
          res_vec(num_members),
          p2p_buf_size(old_connections.p2p_buf_size),
          prev_mode(num_members) {
    old_connections.shutdown_threads();
    //Figure out my SST index
    my_index = (uint32_t)-1;
    for(uint32_t i = 0; i < num_members; ++i) {
        if(members[i] == my_node_id) {
            my_index = i;
        }
        node_id_to_rank[members[i]] = i;
        node_id_to_ip_addr[members[i]] = ip_addr_new_members[i];
    }

    // HARD-CODED. Adding another request type will break this
    window_sizes[P2P_REPLY] = old_connections.window_sizes[P2P_REPLY];
    window_sizes[P2P_REQUEST] = old_connections.window_sizes[P2P_REQUEST];
    window_sizes[RPC_REPLY] = old_connections.window_sizes[RPC_REPLY];
    max_msg_sizes[P2P_REPLY] = old_connections.max_msg_sizes[P2P_REPLY];
    max_msg_sizes[P2P_REQUEST] = old_connections.max_msg_sizes[P2P_REQUEST];
    max_msg_sizes[RPC_REPLY] = old_connections.max_msg_sizes[RPC_REPLY];

    for(uint8_t i = 0; i < num_request_types; ++i) {
        offsets[i] = old_connections.offsets[i];
    }

    for(auto type : p2p_request_types) {
        incoming_seq_nums_map.try_emplace(type, std::vector<std::atomic<uint64_t>>(num_members));
        outgoing_seq_nums_map.try_emplace(type, std::vector<std::atomic<uint64_t>>(num_members));
    }

    for(uint i = 0; i < num_members; ++i) {
        if(old_connections.node_id_to_rank.find(members[i]) != old_connections.node_id_to_rank.end()) {
            auto old_rank = old_connections.node_id_to_rank[members[i]];
            incoming_p2p_buffers[i] = std::move(old_connections.incoming_p2p_buffers[old_rank]);
            outgoing_p2p_buffers[i] = std::move(old_connections.outgoing_p2p_buffers[old_rank]);
            for(auto type : p2p_request_types) {
                incoming_seq_nums_map[type][i].store(old_connections.incoming_seq_nums_map[type][old_rank]);
                outgoing_seq_nums_map[type][i].store(old_connections.outgoing_seq_nums_map[type][old_rank]);
            }
            if(i != my_index) {
                res_vec[i] = std::move(old_connections.res_vec[old_rank]);
            }
        }
    }

    timeout_thread = std::thread(&P2PConnections::check_failures_loop, this);
    tcp_connections_thread = std::thread(&P2PConnections::check_tcp_connections, this);
    tcp_connections_thread.detach();
    // std::cout << "TCP Conn thread started" << std::endl;
}

P2PConnections::~P2PConnections() {
    shutdown_threads();
}

void P2PConnections::shutdown_threads() {
    if(thread_shutdown) {
        return;
    }
    thread_shutdown = true;
    if(timeout_thread.joinable()) {
        timeout_thread.join();
    }
    // std::cout << "Timeout thread terminated" << std::endl;

    tcp::socket s(node_id_to_ip_addr[my_node_id], tcp_port);
    node_id_t id;
    s.exchange(my_node_id, id);
    if(tcp_connections_thread.joinable()) {
        tcp_connections_thread.join();
    }
//     std::cout << "TCP Conn thread terminated" << std::endl;
}

uint32_t P2PConnections::get_node_rank(uint32_t node_id) {
    return node_id_to_rank.at(node_id);
}

uint64_t P2PConnections::get_max_p2p_reply_size() {
    return max_msg_sizes[P2P_REPLY] - sizeof(uint64_t);
}

uint64_t P2PConnections::getOffsetSeqNum(REQUEST_TYPE type, uint64_t seq_num) {
    return offsets[type] + max_msg_sizes[type] * ((seq_num % window_sizes[type]) + 1) - sizeof(uint64_t);
    // return max_msg_size * (type * window_size + (seq_num % window_size) + 1) - sizeof(uint64_t);
}

uint64_t P2PConnections::getOffsetBuf(REQUEST_TYPE type, uint64_t seq_num) {
    return offsets[type] + max_msg_sizes[type] * (seq_num % window_sizes[type]);
    // return max_msg_size * (type * window_size + (seq_num % window_size));
}

// check if there's a new request from some node
char* P2PConnections::probe(uint32_t rank) {
    for(auto type : p2p_request_types) {
        if((uint64_t&)incoming_p2p_buffers[rank][getOffsetSeqNum(type, incoming_seq_nums_map[type][rank])]
           == incoming_seq_nums_map[type][rank] + 1) {
            last_type = type;
            last_rank = rank;
            return const_cast<char*>(incoming_p2p_buffers[rank].get())
                   + getOffsetBuf(type, incoming_seq_nums_map[type][rank]);
        }
    }
    return nullptr;
}

void P2PConnections::update_incoming_seq_num() {
    incoming_seq_nums_map[last_type][last_rank]++;
}

// check if there's a new request from any node
std::optional<std::pair<uint32_t, char*>> P2PConnections::probe_all() {
    for(uint rank = 0; rank < num_members; ++rank) {
        if(incoming_p2p_buffers[rank] != nullptr) {
            auto buf = probe(rank);
            if(buf && buf[0]) {
                return std::pair<uint32_t, char*>(members[rank], buf);
            } else if(buf) {
                // this means that we have a null reply
                // we don't need to process it, but we still want to increment the seq num
                update_incoming_seq_num();
            }
        }
    }
    return {};
}

char* P2PConnections::get_sendbuffer_ptr(uint32_t rank, REQUEST_TYPE type) {
    prev_mode[rank] = type;
    if(type != REQUEST_TYPE::P2P_REQUEST
       || static_cast<int32_t>(incoming_seq_nums_map[REQUEST_TYPE::P2P_REPLY][rank])
                  > static_cast<int32_t>(outgoing_seq_nums_map[REQUEST_TYPE::P2P_REQUEST][rank] - window_sizes[P2P_REQUEST])) {
        // std::cout << "In P2PConnections::get_sendbuffer_ptr with rank = " << rank << ", type = " << type << std::endl;

        // lazy allocation of the buffer
        if(outgoing_p2p_buffers[rank] == nullptr) {
            init_p2p_buffers(rank, true);
        }
        assert(outgoing_p2p_buffers[rank]);

        (uint64_t&)outgoing_p2p_buffers[rank][getOffsetSeqNum(type, outgoing_seq_nums_map[type][rank])]
                = outgoing_seq_nums_map[type][rank] + 1;
        return const_cast<char*>(outgoing_p2p_buffers[rank].get())
               + getOffsetBuf(type, outgoing_seq_nums_map[type][rank]);
    }
    return nullptr;
}

void P2PConnections::send(uint32_t rank) {
    auto type = prev_mode[rank];
    if(rank == my_index) {
        // there's no reason why memcpy shouldn't also copy guard and data separately
        std::memcpy(const_cast<char*>(incoming_p2p_buffers[rank].get()) + getOffsetBuf(type, outgoing_seq_nums_map[type][rank]),
                    const_cast<char*>(outgoing_p2p_buffers[rank].get()) + getOffsetBuf(type, outgoing_seq_nums_map[type][rank]),
                    max_msg_sizes[type] - sizeof(uint64_t));
        std::memcpy(const_cast<char*>(incoming_p2p_buffers[rank].get()) + getOffsetSeqNum(type, outgoing_seq_nums_map[type][rank]),
                    const_cast<char*>(outgoing_p2p_buffers[rank].get()) + getOffsetSeqNum(type, outgoing_seq_nums_map[type][rank]),
                    sizeof(uint64_t));
    } else {
        res_vec[rank]->post_remote_write(getOffsetBuf(type, outgoing_seq_nums_map[type][rank]),
                                         max_msg_sizes[type] - sizeof(uint64_t));
        res_vec[rank]->post_remote_write(getOffsetSeqNum(type, outgoing_seq_nums_map[type][rank]),
                                         sizeof(uint64_t));
        num_rdma_writes++;
    }
    outgoing_seq_nums_map[type][rank]++;
}

void P2PConnections::init_p2p_buffers(uint32_t rank, bool initiator) {
    incoming_p2p_buffers[rank] = std::make_unique<volatile char[]>(p2p_buf_size);
    outgoing_p2p_buffers[rank] = std::make_unique<volatile char[]>(p2p_buf_size);

    if(rank != my_index) {
        // contact the other part
        if(initiator) {
            std::string server_ip = node_id_to_ip_addr[node_id_to_rank[rank]];
            // std::cout << "Contacting node " << node_id_to_rank[rank] << " (rank " << rank << ") (IP: " << server_ip << ")" << std::endl;
            tcp::socket s(server_ip, tcp_port);
            uint32_t remote_id;
            s.exchange<uint32_t>(my_node_id, remote_id);
        }
        // init resources
#ifdef USE_VERBS_API
        res_vec[rank] = std::make_unique<resources>(members[rank], const_cast<char*>(incoming_p2p_buffers[rank].get()),
                                                    const_cast<char*>(outgoing_p2p_buffers[rank].get()),
                                                    p2p_buf_size, p2p_buf_size);
#else
        res_vec[rank] = std::make_unique<resources>(members[rank], const_cast<char*>(incoming_p2p_buffers[rank].get()),
                                                    const_cast<char*>(outgoing_p2p_buffers[rank].get()),
                                                    p2p_buf_size, p2p_buf_size, rank > my_index);
#endif
        // std::cout << "Established connection with remote id " << members[rank] << std::endl;
    }
}

void P2PConnections::check_failures_loop() {
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
        struct verbs_sender_ctxt sctxt[num_members];
#else
        struct lf_sender_ctxt sctxt[num_members];
#endif

        for(uint rank = 0; rank < num_members; ++rank) {
            if(rank == my_index) {
                continue;
            }

            sctxt[rank].remote_id = rank;
            sctxt[rank].ce_idx = ce_idx;

            res_vec[rank]->post_remote_write_with_completion(&sctxt[rank], p2p_buf_size - sizeof(bool), sizeof(bool));
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
        while(num_completions < num_members - 1) {
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

void P2PConnections::check_tcp_connections() {
    tcp::connection_listener cl(tcp_port);
    tcp::socket s;

    while(true) {
        s = cl.accept();
        node_id_t client_id;
        s.exchange(my_node_id, client_id);
        uint32_t remote_rank = node_id_to_rank.at(client_id);
        // std::cout << "Received a connection request from node id " << client_id << std::endl;
        assert(!outgoing_p2p_buffers[remote_rank]);
        if(client_id == my_node_id) {
            break;
        } else {
            init_p2p_buffers(remote_rank, false);
        }
    }
}

void P2PConnections::debug_print() {
    std::cout << "Members: " << std::endl;
    for(auto m : members) {
        std::cout << m << " ";
    }
    std::cout << std::endl;

    for(const auto& type : p2p_request_types) {
        std::cout << "P2PConnections: Request type " << type << std::endl;
        for(uint32_t node = 0; node < num_members; ++node) {
            std::cout << "Node " << node << std::endl;
            std::cout << "incoming seq_nums:";
            for(uint32_t i = 0; i < window_sizes[type]; ++i) {
                uint64_t offset = max_msg_sizes[type] * (type * window_sizes[type] + i + 1) - sizeof(uint64_t);
                std::cout << " " << (uint64_t&)incoming_p2p_buffers[node][offset];
            }
            std::cout << std::endl
                      << "outgoing seq_nums:";
            for(uint32_t i = 0; i < window_sizes[type]; ++i) {
                uint64_t offset = max_msg_sizes[type] * (type * window_sizes[type] + i + 1) - sizeof(uint64_t);
                std::cout << " " << (uint64_t&)outgoing_p2p_buffers[node][offset];
            }
            std::cout << std::endl;
        }
    }
}
}  // namespace sst
