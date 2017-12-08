#include <map>

#include "p2p_connections.h"

P2PConnections::P2PConnections(const P2PParams& params)
        : members(params.members),
          num_members(members.size()),
          my_node_id(params.my_node_id),
          window_size(params.window_size),
          max_msg_size(params.max_p2p_size + sizeof(uint64_t)),
          incoming_p2p_buffers(num_members),
          outgoing_p2p_buffers(num_members),
          res_vec(num_members),
          incoming_request_seq_nums(num_members),
          incoming_reply_seq_nums(num_members),
          outgoing_request_seq_nums(num_members),
          outgoing_reply_seq_nums(num_members),
          prev_mode(num_members) {
    //Figure out my SST index
    my_index = -1;
    for(uint32_t i = 0; i < num_members; ++i) {
        if(members[i] == my_node_id) {
            my_index = i;
            break;
        }
    }
    assert(my_index != -1);

    for(uint i = 0; i < num_members; ++i) {
        if(i == my_index) {
            continue;
        }
        incoming_p2p_buffers[i] = std::make_unique<volatile char[]>(2 * max_msg_size * window_size, 0);
        outgoing_p2p_buffers[i] = std::make_unique<volatile char[]>(2 * max_msg_size * window_size, 0);
        res_vec[i] = std::make_unique<resources_one_sided>(i, incoming_p2p_buffers[i].get(), outgoing_p2p_buffers[i].get(), 2 * max_msg_size * window_size + sizeof(bool), 2 * max_msg_size * window_size + sizeof(bool));
    }
}

P2PConnections::P2PConnections(P2PConnections&& old_connections, const P2PParams& params)
        : members(params.members),
          num_members(members.size()),
          my_node_id(params.my_node_id),
          window_size(params.window_size),
          max_msg_size(params.max_p2p_size + sizeof(uint64_t)),
          incoming_p2p_buffers(num_members),
          outgoing_p2p_buffers(num_members),
          res_vec(num_members),
          incoming_request_seq_nums(num_members),
          incoming_reply_seq_nums(num_members),
          outgoing_request_seq_nums(num_members),
          outgoing_reply_seq_nums(num_members),
          prev_mode(num_members) {
    //Figure out my SST index
    my_index = -1;
    for(uint32_t i = 0; i < num_members; ++i) {
        if(members[i] == my_node_id) {
            my_index = i;
            break;
        }
    }
    assert(my_index != -1);

    std::map<uint32_t, uint32_t> node_id_to_rank;
    for(uint rank = 0; rank < old_connections.num_members; ++rank) {
        node_id_to_rank[old_connections.members[rank]] = rank;
    }

    for(uint i = 0; i < num_members; ++i) {
        if(i == my_index) {
            continue;
        }
        if(node_id_to_rank.find(members[i]) == node_id_to_rank.end()) {
            incoming_p2p_buffers[i] = std::make_unique<volatile char[]>(2 * max_msg_size * window_size, 0);
            outgoing_p2p_buffers[i] = std::make_unique<volatile char[]>(2 * max_msg_size * window_size, 0);
            res_vec[i] = std::make_unique<resources_one_sided>(i, incoming_p2p_buffers[i].get(), outgoing_p2p_buffers[i].get(),
                                                               2 * max_msg_size * window_size + sizeof(bool), 2 * max_msg_size * window_size + sizeof(bool));
        } else {
            auto old_rank = node_id_to_rank[members[i]];
            incoming_p2p_buffers[i] = std::move(old_connections.incoming_p2p_buffers(old_rank));
            outgoing_p2p_buffers[i] = std::move(old_connections.outgoing_p2p_buffers(old_rank));
            res_vec[i] = std::move(old_connections.res_vec[old_rank]);
	    incoming_request_seq_nums[i] = old_connections.incoming_request_seq_nums[old_rank];
	    incoming_reply_seq_nums[i] = old_connections.incoming_reply_seq_nums[old_rank];
	    outgoing_request_seq_nums[i] = old_connections.outgoing_request_seq_nums[old_rank];
	    outgoing_reply_seq_nums[i] = old_connections.outgoing_reply_seq_nums[old_rank];
        }
    }
}

// check if there's a new request from some node
volatile char* P2PConnections::probe(uint32_t rank) {
    // first check for replies
    if((uint64_t)incoming_p2p_buffers[rank][max_msg_size * (window_size + (incoming_reply_seq_nums[rank] % window_size) + 1) - sizeof(uint64_t)] == incoming_reply_seq_nums[rank] + 1) {
        return incoming_p2p_buffers[rank][max_msg_size * (window_size + (incoming_reply_seq_nums[rank]++ % window_size))];
    }
    if((uint64_t)incoming_p2p_buffers[rank][max_msg_size * (incoming_request_seq_nums[rank] % window_size + 1) - sizeof(uint64_t)] == incoming_request_seq_nums[rank] + 1) {
        return incoming_p2p_buffers[rank][max_msg_size * (incoming_request_seq_nums[rank]++ % window_size)];
    }
    return nullptr;
}

// check if there's a new request from any node
pair<uint32_t, volatile char*> P2PConnections::probe_all() {
    for(uint rank = 0; rank < num_members; ++rank) {
        if(rank == my_index) {
            continue;
        }
        auto buf = probe(rank);
        if(buf) {
            return pair<uint32_t, volatile char*>(rank, buf);
        }
    }
}

volatile char* P2PConnections::get_sendbuffer_ptr(uint32_t rank, bool reply = false) {
    prev_mode[rank] = reply;
    if(reply) {
        (uint64_t&)outgoing_p2p_buffers[rank][max_msg_size * (window_size + (outgoing_reply_seq_nums[rank] % window_size) + 1) - sizeof(uint64_t)] = outgoing_reply_seq_nums[rank] + 1;
        return outgoing_p2p_buffers[rank][max_msg_size * (window_size + (outgoing_reply_seq_nums[rank] % window_size))];
    } else {
        if(incoming_reply_seq_nums[rank] > outgoing_request_seq_nums[rank] - window_size) {
            (uint64_t&)outgoing_p2p_buffers[rank][max_msg_size * (outgoing_request_seq_nums[rank] % window_size + 1) - sizeof(uint64_t)] = outgoing_request_seq_nums[rank] + 1;
            return outgoing_p2p_buffers[rank][max_msg_size * (outgoing_request_seq_nums[rank] % window_size)];
        } else {
            return nullptr;
        }
    }
}

void P2PConnections::send(uint32_t rank) {
    num_puts++;
    if(prev_mode[rank]) {
        if(num_puts == 1000) {
            const auto tid = std::this_thread::get_id();
            // get id first
            uint32_t id = util::polling_data.get_index(tid);

            util::polling_data.set_waiting(tid);
            res_vec[rank]->post_remote_write_with_completion(id, max_msg_size * (window_size + (outgoing_reply_seq_nums[rank] % window_size)), max_msg_size);
            outgoing_reply_seq_nums[rank]++;
            num_puts = 0;
            /** Completion Queue poll timeout in millisec */
            const int MAX_POLL_CQ_TIMEOUT = 2000;
            unsigned long start_time_msec;
            unsigned long cur_time_msec;
            struct timeval cur_time;
            std::experimental::optional<std::pair<int32_t, int32_t>> ce;
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
        } else {
            res_vec[rank]->post_remote_write(0, max_msg_size * (window_size + (outgoing_reply_seq_nums[rank] % window_size)), max_msg_size);
            outgoing_reply_seq_nums[rank]++;
        }
    } else {
        if(num_puts == 1000) {
            const auto tid = std::this_thread::get_id();
            // get id first
            uint32_t id = util::polling_data.get_index(tid);

            util::polling_data.set_waiting(tid);
            res_vec[rank]->post_remote_write(id, max_msg_size * (outgoing_request_seq_nums[rank] % window_size), max_msg_size);
            outgoing_request_seq_nums[rank]++;
            num_puts = 0;
            /** Completion Queue poll timeout in millisec */
            const int MAX_POLL_CQ_TIMEOUT = 2000;
            unsigned long start_time_msec;
            unsigned long cur_time_msec;
            struct timeval cur_time;
            std::experimental::optional<std::pair<int32_t, int32_t>> ce;
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
        } else {
            res_vec[rank]->post_remote_write(0, max_msg_size * (outgoing_request_seq_nums[rank] % window_size), max_msg_size);
            outgoing_request_seq_nums[rank]++;
        }
    }
}
