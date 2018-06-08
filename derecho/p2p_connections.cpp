#include <map>

#include <cassert>
#include <sys/time.h>

#include "p2p_connections.h"
#include "sst/poll_utils.h"

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
          incoming_request_seq_nums(num_members),
          incoming_rpc_reply_seq_nums(num_members),
          incoming_p2p_reply_seq_nums(num_members),
          outgoing_request_seq_nums(num_members),
          outgoing_rpc_reply_seq_nums(num_members),
          outgoing_p2p_reply_seq_nums(num_members),
          prev_mode(num_members) {
    std::cout << "this=" << this << std::endl;
    //Figure out my SST index
    my_index = (uint32_t)-1;
    for(uint32_t i = 0; i < num_members; ++i) {
        if(members[i] == my_node_id) {
            my_index = i;
        }
        node_id_to_rank[members[i]] = i;
    }
    assert(my_index != (uint32_t) -1);

    for(uint i = 0; i < num_members; ++i) {
        incoming_p2p_buffers[i] = std::make_unique<volatile char[]>(3 * max_msg_size * window_size + sizeof(bool));
        outgoing_p2p_buffers[i] = std::make_unique<volatile char[]>(3 * max_msg_size * window_size + sizeof(bool));
        if(i == my_index) {
            continue;
        }
        res_vec[i] = std::make_unique<resources_one_sided>(i, const_cast<char*>(incoming_p2p_buffers[i].get()), const_cast<char*>(outgoing_p2p_buffers[i].get()), 3 * max_msg_size * window_size + sizeof(bool), 3 * max_msg_size * window_size + sizeof(bool));
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
          incoming_request_seq_nums(num_members),
          incoming_rpc_reply_seq_nums(num_members),
          incoming_p2p_reply_seq_nums(num_members),
          outgoing_request_seq_nums(num_members),
          outgoing_rpc_reply_seq_nums(num_members),
          outgoing_p2p_reply_seq_nums(num_members),
          prev_mode(num_members) {
    std::cout << "this=" << this << std::endl;
    //Figure out my SST index
    my_index = (uint32_t)-1;
    for(uint32_t i = 0; i < num_members; ++i) {
        if(members[i] == my_node_id) {
            my_index = i;
        }
        node_id_to_rank[members[i]] = i;
    }
    assert(my_index != (uint32_t) -1);

    std::map<uint32_t, uint32_t> node_id_to_rank;
    for(uint rank = 0; rank < old_connections.num_members; ++rank) {
        node_id_to_rank[old_connections.members[rank]] = rank;
    }

    for(uint i = 0; i < num_members; ++i) {
        if(i == my_index) {
            continue;
        }
        if(node_id_to_rank.find(members[i]) == node_id_to_rank.end()) {
	  incoming_p2p_buffers[i] = std::make_unique<volatile char[]>(3 * max_msg_size * window_size + sizeof(bool));
            outgoing_p2p_buffers[i] = std::make_unique<volatile char[]>(3 * max_msg_size * window_size + sizeof(bool));
            res_vec[i] = std::make_unique<resources_one_sided>(i, const_cast<char*>(incoming_p2p_buffers[i].get()), const_cast<char*>(outgoing_p2p_buffers[i].get()),
                                                               3 * max_msg_size * window_size + sizeof(bool), 3 * max_msg_size * window_size + sizeof(bool));
        } else {
            auto old_rank = node_id_to_rank[members[i]];
	    incoming_p2p_buffers[i] = std::move(old_connections.incoming_p2p_buffers[old_rank]);
            outgoing_p2p_buffers[i] = std::move(old_connections.outgoing_p2p_buffers[old_rank]);
            res_vec[i] = std::move(old_connections.res_vec[old_rank]);
            incoming_request_seq_nums[i] = old_connections.incoming_request_seq_nums[old_rank];
            incoming_rpc_reply_seq_nums[i] = old_connections.incoming_rpc_reply_seq_nums[old_rank];
            incoming_p2p_reply_seq_nums[i] = old_connections.incoming_p2p_reply_seq_nums[old_rank];
            outgoing_request_seq_nums[i] = old_connections.outgoing_request_seq_nums[old_rank];
            outgoing_rpc_reply_seq_nums[i] = old_connections.outgoing_rpc_reply_seq_nums[old_rank];
            outgoing_p2p_reply_seq_nums[i] = old_connections.outgoing_p2p_reply_seq_nums[old_rank];
        }
    }

    timeout_thread = std::thread(&P2PConnections::check_failures_loop, this);
}

P2PConnections::~P2PConnections() {
    std::cout << "In the P2PConnections destructor" << std::endl;
    thread_shutdown = true;
    if (timeout_thread.joinable()) {
      timeout_thread.join();
    }
}

uint32_t P2PConnections::get_node_rank(uint32_t node_id) {
    return node_id_to_rank.at(node_id);
}

uint64_t P2PConnections::get_max_p2p_size() {
    return max_msg_size - sizeof(uint64_t);
}

// check if there's a new request from some node
char* P2PConnections::probe(uint32_t rank) {
    // first check for RPC replies
    if((uint64_t)incoming_p2p_buffers[rank][max_msg_size * (2 * window_size + (incoming_rpc_reply_seq_nums[rank] % window_size) + 1) - sizeof(uint64_t)] == incoming_rpc_reply_seq_nums[rank] + 1) {
      return const_cast<char*>(incoming_p2p_buffers[rank].get()) + max_msg_size * (2 * window_size + (incoming_rpc_reply_seq_nums[rank]++ % window_size));
    }
    // then check for P2P replies
    if((uint64_t)incoming_p2p_buffers[rank][max_msg_size * (window_size + (incoming_p2p_reply_seq_nums[rank] % window_size) + 1) - sizeof(uint64_t)] == incoming_p2p_reply_seq_nums[rank] + 1) {
      return const_cast<char*>(incoming_p2p_buffers[rank].get()) + max_msg_size * (window_size + (incoming_p2p_reply_seq_nums[rank]++ % window_size));
    }
    // finally check for any new requests
    if((uint64_t)incoming_p2p_buffers[rank][max_msg_size * (incoming_request_seq_nums[rank] % window_size + 1) - sizeof(uint64_t)] == incoming_request_seq_nums[rank] + 1) {
      return const_cast<char*>(incoming_p2p_buffers[rank].get()) + max_msg_size * (incoming_request_seq_nums[rank]++ % window_size);
    }
    return nullptr;
}

// check if there's a new request from any node
std::experimental::optional<std::pair<uint32_t, char*>> P2PConnections::probe_all() {
    for(uint rank = 0; rank < num_members; ++rank) {
        if(rank == my_index) {
            continue;
        }
        auto buf = probe(rank);
        if(buf) {
            return std::pair<uint32_t, char*>(rank, buf);
        }
    }
    return {};
}

char* P2PConnections::get_sendbuffer_ptr(uint32_t rank, REQUEST_TYPE type) {
    prev_mode[rank] = type;
    if(type == REQUEST_TYPE::RPC_REPLY) {
        (uint64_t&)outgoing_p2p_buffers[rank][max_msg_size * (2 * window_size + (outgoing_rpc_reply_seq_nums[rank] % window_size) + 1) - sizeof(uint64_t)] = outgoing_rpc_reply_seq_nums[rank] + 1;
        return const_cast<char*>(outgoing_p2p_buffers[rank].get()) + max_msg_size * (2 * window_size + (outgoing_rpc_reply_seq_nums[rank] % window_size));
    } else if(type == REQUEST_TYPE::P2P_REPLY) {
        (uint64_t&)outgoing_p2p_buffers[rank][max_msg_size * (window_size + (outgoing_p2p_reply_seq_nums[rank] % window_size) + 1) - sizeof(uint64_t)] = outgoing_p2p_reply_seq_nums[rank] + 1;
        return const_cast<char*>(outgoing_p2p_buffers[rank].get()) + max_msg_size * (window_size + (outgoing_p2p_reply_seq_nums[rank] % window_size));
    } else {
        if(incoming_p2p_reply_seq_nums[rank] > outgoing_request_seq_nums[rank] - window_size) {
            (uint64_t&)outgoing_p2p_buffers[rank][max_msg_size * (outgoing_request_seq_nums[rank] % window_size + 1) - sizeof(uint64_t)] = outgoing_request_seq_nums[rank] + 1;
            return const_cast<char*>(outgoing_p2p_buffers[rank].get()) + max_msg_size * (outgoing_request_seq_nums[rank] % window_size);
        } else {
            return nullptr;
        }
    }
}

void P2PConnections::send(uint32_t rank) {
    if(prev_mode[rank] == REQUEST_TYPE::RPC_REPLY) {
        res_vec[rank]->post_remote_write(0, max_msg_size * (2 * window_size + (outgoing_rpc_reply_seq_nums[rank] % window_size)), max_msg_size);
        outgoing_rpc_reply_seq_nums[rank]++;
    } else if(prev_mode[rank] == REQUEST_TYPE::P2P_REPLY) {
        res_vec[rank]->post_remote_write(0, max_msg_size * (window_size + (outgoing_p2p_reply_seq_nums[rank] % window_size)), max_msg_size);
        outgoing_p2p_reply_seq_nums[rank]++;
    } else {
        res_vec[rank]->post_remote_write(0, max_msg_size * (outgoing_request_seq_nums[rank] % window_size), max_msg_size);
        outgoing_request_seq_nums[rank]++;
    }
}

void P2PConnections::check_failures_loop() {
    pthread_setname_np(pthread_self(), "p2p_timeout_thread");
    const auto tid = std::this_thread::get_id();
    // get id first
    uint32_t id = util::polling_data.get_index(tid);
    while(!thread_shutdown) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));

        util::polling_data.set_waiting(tid);

        for(uint rank = 0; rank < num_members; ++rank) {
            if(rank == my_index) {
                continue;
            }

            res_vec[rank]->post_remote_write_with_completion(id, max_msg_size * 3 * window_size, sizeof(bool));
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
            if(!ce) {
                break;
            }
            num_completions++;
        }
        util::polling_data.reset_waiting(tid);
    }
}
}  // namespace sst
