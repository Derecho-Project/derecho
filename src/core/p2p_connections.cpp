#include <map>

#include <cassert>
#include <cstring>
#include <sys/time.h>

#include <derecho/core/detail/p2p_connections.hpp>
#include <derecho/sst/detail/poll_utils.hpp>

namespace sst {
P2PConnections::P2PConnections(const P2PParams params)
        : members(params.members),
          num_members(members.size()),
          my_node_id(params.my_node_id),
          window_size(params.window_size),
          max_msg_size(params.max_p2p_size + sizeof(uint64_t)),
          incoming_p2p_buffers(num_members),
          outgoing_p2p_buffers(num_members),
          res_vec(num_members),
          p2p_buf_size(4 * max_msg_size * window_size + sizeof(bool)),
          incoming_send_seq_nums(num_members),
          incoming_rpc_reply_seq_nums(num_members),
          incoming_p2p_reply_seq_nums(num_members),
          outgoing_send_seq_nums(num_members),
          outgoing_rpc_reply_seq_nums(num_members),
          outgoing_p2p_reply_seq_nums(num_members),
          prev_mode(num_members) {
    //Figure out my SST index
    my_index = (uint32_t)-1;
    for(uint32_t i = 0; i < num_members; ++i) {
        if(members[i] == my_node_id) {
            my_index = i;
        }
        node_id_to_rank[members[i]] = i;
    }
    assert(my_index != (uint32_t)-1);

    for(uint i = 0; i < num_members; ++i) {
        incoming_p2p_buffers[i] = std::make_unique<volatile char[]>(p2p_buf_size);
        outgoing_p2p_buffers[i] = std::make_unique<volatile char[]>(p2p_buf_size);
        if(i != my_index) {
#ifdef USE_VERBS_API
            res_vec[i] = std::make_unique<resources>(i, const_cast<char*>(incoming_p2p_buffers[i].get()),
                                                     const_cast<char*>(outgoing_p2p_buffers[i].get()),
                                                     p2p_buf_size, p2p_buf_size);
#else
            res_vec[i] = std::make_unique<resources>(i, const_cast<char*>(incoming_p2p_buffers[i].get()),
                                                     const_cast<char*>(outgoing_p2p_buffers[i].get()),
                                                     p2p_buf_size, p2p_buf_size, i > my_index);
#endif
        }
    }

    timeout_thread = std::thread(&P2PConnections::check_failures_loop, this);
}

P2PConnections::P2PConnections(P2PConnections&& old_connections, const std::vector<uint32_t> new_members)
        : members(new_members),
          num_members(members.size()),
          my_node_id(old_connections.my_node_id),
          window_size(old_connections.window_size),
          max_msg_size(old_connections.max_msg_size),
          incoming_p2p_buffers(num_members),
          outgoing_p2p_buffers(num_members),
          res_vec(num_members),
          p2p_buf_size(4 * max_msg_size * window_size + sizeof(bool)),
          incoming_send_seq_nums(num_members),
          incoming_rpc_reply_seq_nums(num_members),
          incoming_p2p_reply_seq_nums(num_members),
          outgoing_send_seq_nums(num_members),
          outgoing_rpc_reply_seq_nums(num_members),
          outgoing_p2p_reply_seq_nums(num_members),
          prev_mode(num_members) {
    old_connections.shutdown_failures_thread();
    //Figure out my SST index
    my_index = (uint32_t)-1;
    for(uint32_t i = 0; i < num_members; ++i) {
        if(members[i] == my_node_id) {
            my_index = i;
        }
        node_id_to_rank[members[i]] = i;
    }
    assert(my_index != (uint32_t)-1);

    for(uint i = 0; i < num_members; ++i) {
        if(old_connections.node_id_to_rank.find(members[i]) == old_connections.node_id_to_rank.end()) {
            incoming_p2p_buffers[i] = std::make_unique<volatile char[]>(p2p_buf_size);
            outgoing_p2p_buffers[i] = std::make_unique<volatile char[]>(p2p_buf_size);
            if(i != my_index) {
                res_vec[i] = std::make_unique<resources>(members[i], const_cast<char*>(incoming_p2p_buffers[i].get()),
                                                         const_cast<char*>(outgoing_p2p_buffers[i].get()),
                                                         p2p_buf_size, p2p_buf_size, i > my_index);
            }
        } else {
            auto old_rank = old_connections.node_id_to_rank[members[i]];
            incoming_p2p_buffers[i] = std::move(old_connections.incoming_p2p_buffers[old_rank]);
            outgoing_p2p_buffers[i] = std::move(old_connections.outgoing_p2p_buffers[old_rank]);
            incoming_send_seq_nums[i] = old_connections.incoming_send_seq_nums[old_rank];
            incoming_rpc_reply_seq_nums[i] = old_connections.incoming_rpc_reply_seq_nums[old_rank];
            incoming_p2p_reply_seq_nums[i] = old_connections.incoming_p2p_reply_seq_nums[old_rank];
            outgoing_send_seq_nums[i] = old_connections.outgoing_send_seq_nums[old_rank];
            outgoing_rpc_reply_seq_nums[i] = old_connections.outgoing_rpc_reply_seq_nums[old_rank];
            outgoing_p2p_reply_seq_nums[i] = old_connections.outgoing_p2p_reply_seq_nums[old_rank];
            if(i != my_index) {
                res_vec[i] = std::move(old_connections.res_vec[old_rank]);
            }
        }
    }

    timeout_thread = std::thread(&P2PConnections::check_failures_loop, this);
}

P2PConnections::~P2PConnections() {
    shutdown_failures_thread();
}

void P2PConnections::shutdown_failures_thread() {
    thread_shutdown = true;
    if(timeout_thread.joinable()) {
        timeout_thread.join();
    }
}

uint32_t P2PConnections::get_node_rank(uint32_t node_id) {
    return node_id_to_rank.at(node_id);
}

uint64_t P2PConnections::get_max_p2p_size() {
    return max_msg_size - sizeof(uint64_t);
}

uint64_t P2PConnections::getOffsetSeqNum(REQUEST_TYPE type, uint64_t seq_num) {
    switch(type) {
        case REQUEST_TYPE::RPC_REPLY:
            return max_msg_size * (2 * window_size + (seq_num % window_size) + 1) - sizeof(uint64_t);
        case REQUEST_TYPE::P2P_REPLY:
            return max_msg_size * (window_size + (seq_num % window_size) + 1) - sizeof(uint64_t);
        case REQUEST_TYPE::P2P_SEND:
            return max_msg_size * (seq_num % window_size + 1) - sizeof(uint64_t);
    }
    return 0;
}

// note that it takes seq_num by reference and increments it!
uint64_t P2PConnections::getOffsetBuf(REQUEST_TYPE type, uint64_t& seq_num) {
    switch(type) {
        case REQUEST_TYPE::RPC_REPLY:
            return max_msg_size * (2 * window_size + (seq_num++ % window_size));
        case REQUEST_TYPE::P2P_REPLY:
            return max_msg_size * (window_size + (seq_num++ % window_size));
        case REQUEST_TYPE::P2P_SEND:
            return max_msg_size * (seq_num++ % window_size);
    }
    return 0;
}

uint64_t P2PConnections::getOffsetBufNoIncrement(REQUEST_TYPE type, uint64_t seq_num) {
    switch(type) {
        case REQUEST_TYPE::RPC_REPLY:
            return max_msg_size * (2 * window_size + (seq_num % window_size));
        case REQUEST_TYPE::P2P_REPLY:
            return max_msg_size * (window_size + (seq_num % window_size));
        case REQUEST_TYPE::P2P_SEND:
            return max_msg_size * (seq_num % window_size);
    }
    return 0;
}

// check if there's a new request from some node
char* P2PConnections::probe(uint32_t rank) {
    assert(incoming_p2p_buffers[rank]);
    // first check for RPC replies
    if((uint64_t&)incoming_p2p_buffers[rank][getOffsetSeqNum(REQUEST_TYPE::RPC_REPLY, incoming_rpc_reply_seq_nums[rank])] == incoming_rpc_reply_seq_nums[rank] + 1) {
        return const_cast<char*>(incoming_p2p_buffers[rank].get()) + getOffsetBuf(REQUEST_TYPE::RPC_REPLY, incoming_rpc_reply_seq_nums[rank]);
    }
    // then check for P2P replies
    if((uint64_t&)incoming_p2p_buffers[rank][getOffsetSeqNum(REQUEST_TYPE::P2P_REPLY, incoming_p2p_reply_seq_nums[rank])] == incoming_p2p_reply_seq_nums[rank] + 1) {
        return const_cast<char*>(incoming_p2p_buffers[rank].get()) + getOffsetBuf(REQUEST_TYPE::P2P_REPLY, incoming_p2p_reply_seq_nums[rank]);
    }
    // finally check for any new requests
    if((uint64_t&)incoming_p2p_buffers[rank][getOffsetSeqNum(REQUEST_TYPE::P2P_SEND, incoming_send_seq_nums[rank])] == incoming_send_seq_nums[rank] + 1) {
        return const_cast<char*>(incoming_p2p_buffers[rank].get()) + getOffsetBuf(REQUEST_TYPE::P2P_SEND, incoming_send_seq_nums[rank]);
    }
    return nullptr;
}

// check if there's a new request from any node
std::optional<std::pair<uint32_t, char*>> P2PConnections::probe_all() {
    for(uint rank = 0; rank < num_members; ++rank) {
        auto buf = probe(rank);
        if(buf && buf[0]) {
            return std::pair<uint32_t, char*>(members[rank], buf);
        }
    }
    return {};
}

char* P2PConnections::get_sendbuffer_ptr(uint32_t rank, REQUEST_TYPE type) {
    prev_mode[rank] = type;
    if(type == REQUEST_TYPE::RPC_REPLY) {
        (uint64_t&)outgoing_p2p_buffers[rank][getOffsetSeqNum(type, outgoing_rpc_reply_seq_nums[rank])] = outgoing_rpc_reply_seq_nums[rank] + 1;
        return const_cast<char*>(outgoing_p2p_buffers[rank].get()) + getOffsetBufNoIncrement(type, outgoing_rpc_reply_seq_nums[rank]);
    } else if(type == REQUEST_TYPE::P2P_REPLY) {
        (uint64_t&)outgoing_p2p_buffers[rank][getOffsetSeqNum(type, outgoing_p2p_reply_seq_nums[rank])] = outgoing_p2p_reply_seq_nums[rank] + 1;
        return const_cast<char*>(outgoing_p2p_buffers[rank].get()) + getOffsetBufNoIncrement(type, outgoing_p2p_reply_seq_nums[rank]);
    } else {
        if((int32_t)incoming_p2p_reply_seq_nums[rank] > (int32_t)(outgoing_send_seq_nums[rank] - window_size)) {
            (uint64_t&)outgoing_p2p_buffers[rank][getOffsetSeqNum(type, outgoing_send_seq_nums[rank])] = outgoing_send_seq_nums[rank] + 1;
            return const_cast<char*>(outgoing_p2p_buffers[rank].get()) + getOffsetBufNoIncrement(type, outgoing_send_seq_nums[rank]);
        } else {
            return nullptr;
        }
    }
}

void P2PConnections::send(uint32_t rank) {
    if(prev_mode[rank] == REQUEST_TYPE::RPC_REPLY) {
        if(rank == my_index) {
            // there's no reason why memcpy shouldn't also copy guard and data separately
            std::memcpy(const_cast<char*>(incoming_p2p_buffers[rank].get()) + getOffsetBufNoIncrement(prev_mode[rank], outgoing_rpc_reply_seq_nums[rank]),
                        const_cast<char*>(outgoing_p2p_buffers[rank].get()) + getOffsetBufNoIncrement(prev_mode[rank], outgoing_rpc_reply_seq_nums[rank]),
                        max_msg_size - sizeof(uint64_t));
            std::memcpy(const_cast<char*>(incoming_p2p_buffers[rank].get()) + getOffsetSeqNum(prev_mode[rank], outgoing_rpc_reply_seq_nums[rank]),
                        const_cast<char*>(outgoing_p2p_buffers[rank].get()) + getOffsetSeqNum(prev_mode[rank], outgoing_rpc_reply_seq_nums[rank]),
                        sizeof(uint64_t));
        } else {
            res_vec[rank]->post_remote_write(getOffsetBufNoIncrement(prev_mode[rank], outgoing_rpc_reply_seq_nums[rank]), max_msg_size - sizeof(uint64_t));
            res_vec[rank]->post_remote_write(getOffsetSeqNum(prev_mode[rank], outgoing_rpc_reply_seq_nums[rank]), sizeof(uint64_t));
            num_rdma_writes++;
        }
        outgoing_rpc_reply_seq_nums[rank]++;
    } else if(prev_mode[rank] == REQUEST_TYPE::P2P_REPLY) {
        if(rank == my_index) {
            std::memcpy(const_cast<char*>(incoming_p2p_buffers[rank].get()) + getOffsetBufNoIncrement(prev_mode[rank], outgoing_p2p_reply_seq_nums[rank]),
                        const_cast<char*>(outgoing_p2p_buffers[rank].get()) + getOffsetBufNoIncrement(prev_mode[rank], outgoing_p2p_reply_seq_nums[rank]),
                        max_msg_size - sizeof(uint64_t));
            std::memcpy(const_cast<char*>(incoming_p2p_buffers[rank].get()) + getOffsetSeqNum(prev_mode[rank], outgoing_p2p_reply_seq_nums[rank]),
                        const_cast<char*>(outgoing_p2p_buffers[rank].get()) + getOffsetSeqNum(prev_mode[rank], outgoing_p2p_reply_seq_nums[rank]),
                        sizeof(uint64_t));
        } else {
            res_vec[rank]->post_remote_write(getOffsetBufNoIncrement(prev_mode[rank], outgoing_p2p_reply_seq_nums[rank]), max_msg_size - sizeof(uint64_t));
            res_vec[rank]->post_remote_write(getOffsetSeqNum(prev_mode[rank], outgoing_p2p_reply_seq_nums[rank]), sizeof(uint64_t));
            num_rdma_writes++;
        }
	outgoing_p2p_reply_seq_nums[rank]++;
    } else if(prev_mode[rank] == REQUEST_TYPE::P2P_SEND) {
        if(rank == my_index) {
            std::memcpy(const_cast<char*>(incoming_p2p_buffers[rank].get()) + getOffsetBufNoIncrement(prev_mode[rank], outgoing_send_seq_nums[rank]),
                        const_cast<char*>(outgoing_p2p_buffers[rank].get()) + getOffsetBufNoIncrement(prev_mode[rank], outgoing_send_seq_nums[rank]),
                        max_msg_size - sizeof(uint64_t));
            std::memcpy(const_cast<char*>(incoming_p2p_buffers[rank].get()) + getOffsetSeqNum(prev_mode[rank], outgoing_send_seq_nums[rank]),
                        const_cast<char*>(outgoing_p2p_buffers[rank].get()) + getOffsetSeqNum(prev_mode[rank], outgoing_send_seq_nums[rank]),
                        sizeof(uint64_t));
        } else {
            res_vec[rank]->post_remote_write(getOffsetBufNoIncrement(prev_mode[rank], outgoing_send_seq_nums[rank]), max_msg_size - sizeof(uint64_t));
            res_vec[rank]->post_remote_write(getOffsetSeqNum(prev_mode[rank], outgoing_send_seq_nums[rank]), sizeof(uint64_t));
            num_rdma_writes++;
        }
	outgoing_send_seq_nums[rank]++;
    }
}

void P2PConnections::check_failures_loop() {
    pthread_setname_np(pthread_self(), "p2p_timeout_thread");
    const auto tid = std::this_thread::get_id();
    // get id first
    uint32_t ce_idx = util::polling_data.get_index(tid);
    while(!thread_shutdown) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
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
}  // namespace sst
