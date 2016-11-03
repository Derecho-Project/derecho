
#include "rdmc.h"
#include "group_send.h"
#include "message.h"
#include "microbenchmarks.h"
#include "util.h"
#include "verbs_helper.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cinttypes>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <poll.h>
#include <queue>
#include <set>
#include <sstream>
#include <string>
#include <sys/mman.h>
#include <sys/resource.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std;
using namespace rdma;

namespace rdmc {
uint32_t node_rank;
atomic<bool> shutdown_flag;

// map from group number to group
map<uint16_t, shared_ptr<group> > groups;
mutex groups_lock;

bool initialize(const map<uint32_t, string>& addresses, uint32_t _node_rank) {
    if(shutdown_flag) return false;

    node_rank = _node_rank;

    TRACE("starting initialize");
    if(!::rdma::impl::verbs_initialize(addresses, node_rank)) {
        return false;
    }
    TRACE("verbs initialized");

    auto find_group = [](uint16_t group_number) {
        unique_lock<mutex> lock(groups_lock);
        auto it = groups.find(group_number);
        return it != groups.end() ? it->second : nullptr;
    };
    auto send_data_block = [find_group](uint64_t tag, uint32_t immediate,
                                        size_t length) {
        ParsedTag parsed_tag = parse_tag(tag);
        shared_ptr<group> g = find_group(parsed_tag.group_number);
        if(g) g->complete_block_send();
    };
    auto receive_data_block = [find_group](uint64_t tag, uint32_t immediate,
                                           size_t length) {
        ParsedTag parsed_tag = parse_tag(tag);
        shared_ptr<group> g = find_group(parsed_tag.group_number);
        if(g) g->receive_block(immediate, length);
    };
    auto send_ready_for_block = [](uint64_t, uint32_t, size_t) {};
    auto receive_ready_for_block = [find_group](
        uint64_t tag, uint32_t immediate, size_t length) {
        ParsedTag parsed_tag = parse_tag(tag);
        shared_ptr<group> g = find_group(parsed_tag.group_number);
        if(g) g->receive_ready_for_block(immediate, parsed_tag.target);
    };

    group::message_types.data_block =
        message_type("rdmc.data_block", send_data_block, receive_data_block);
    group::message_types.ready_for_block = message_type(
        "rdmc.ready_for_block", send_ready_for_block, receive_ready_for_block);

    return true;
}
void add_address(uint32_t index, const string& address) {
    ::rdma::impl::verbs_add_connection(index, address, node_rank);
}

bool create_group(uint16_t group_number, std::vector<uint32_t> members,
                  size_t block_size, send_algorithm algorithm,
                  incoming_message_callback_t incoming_upcall,
                  completion_callback_t callback,
                  failure_callback_t failure_callback) {
    if(shutdown_flag) return false;

	schedule* send_schedule;
    uint32_t member_index = index_of(members, node_rank);
    if(algorithm == BINOMIAL_SEND) {
        send_schedule = new binomial_schedule(members.size(), member_index);
    } else if(algorithm == SEQUENTIAL_SEND) {
        send_schedule = new sequential_schedule(members.size(), member_index);
    } else if(algorithm == CHAIN_SEND) {
        send_schedule = new chain_schedule(members.size(), member_index);
    } else if(algorithm == TREE_SEND) {
		send_schedule = new tree_schedule(members.size(), member_index);
    } else {
        puts("Unsupported group type?!");
        fflush(stdout);
        return false;
    }

    auto g = make_shared<group>(group_number, block_size, members, member_index,
                                incoming_upcall, callback,
								unique_ptr<schedule>(send_schedule));

    unique_lock<mutex> lock(groups_lock);
    auto p = groups.emplace(group_number, std::move(g));
    return p.second;
}

void destroy_group(uint16_t group_number) {
    if(shutdown_flag) return;

    unique_lock<mutex> lock(groups_lock);
    LOG_EVENT(group_number, -1, -1, "destroy_group");
    groups.erase(group_number);
}
void shutdown() { shutdown_flag = true; }
bool send(uint16_t group_number, shared_ptr<memory_region> mr, size_t offset,
          size_t length) {
    if(shutdown_flag) return false;

    shared_ptr<group> g;
    {
        unique_lock<mutex> lock(groups_lock);
        auto it = groups.find(group_number);
        if(it == groups.end()) return false;
        g = it->second;
    }
    LOG_EVENT(group_number, -1, -1, "preparing_to_send_message");
    g->send_message(mr, offset, length);
    return true;
}

barrier_group::barrier_group(vector<uint32_t> members) {
    member_index = index_of(members, node_rank);
    group_size = members.size();

    if(group_size <= 1 || member_index >= members.size())
        throw rdmc::invalid_args();

    total_steps = ceil(log2(group_size));
    for(unsigned int m = 0; m < total_steps; m++) steps[m] = -1;

    steps_mr = make_unique<memory_region>((char*)&steps[0],
                                          total_steps * sizeof(int64_t));
    number_mr = make_unique<memory_region>((char*)&number, sizeof(number));

    set<uint32_t> targets;
    for(unsigned int m = 0; m < total_steps; m++) {
        auto target = (member_index + (1 << m)) % group_size;
        auto target2 =
            (group_size * (1 << m) + member_index - (1 << m)) % group_size;
        targets.insert(target);
        targets.insert(target2);
    }

    map<uint32_t, queue_pair> qps;
    for(auto target : targets) {
        qps.emplace(target, queue_pair(members[target]));
    }

    auto remote_mrs = ::rdma::impl::verbs_exchange_memory_regions(
        members, node_rank, *steps_mr.get());
    for(unsigned int m = 0; m < total_steps; m++) {
        auto target = (member_index + (1 << m)) % group_size;

        remote_memory_regions.push_back(remote_mrs.find(target)->second);

        auto qp_it = qps.find(target);
        queue_pairs.push_back(std::move(qp_it->second));
        qps.erase(qp_it);
    }

    for(auto it = qps.begin(); it != qps.end(); it++) {
        extra_queue_pairs.push_back(std::move(it->second));
        qps.erase(it);
    }
}
void barrier_group::barrier_wait() {
    // See:
    // http://mvapich.cse.ohio-state.edu/static/media/publications/abstract/kinis-euro03.pdf

    unique_lock<mutex> l(lock);
    LOG_EVENT(-1, -1, -1, "start_barrier");
    number++;

    for(unsigned int m = 0; m < total_steps; m++) {
        if(!queue_pairs[m].post_write(
               *number_mr.get(), 0, 8,
               form_tag(0, (node_rank + (1 << m)) % group_size),
               remote_memory_regions[m], m * 8, false, true)) {
            throw rdmc::connection_broken();
        }

        while(steps[m] < number) /* do nothing*/
            ;
    }
    LOG_EVENT(-1, -1, -1, "end_barrier");
}
}
