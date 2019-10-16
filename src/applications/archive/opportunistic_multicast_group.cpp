#include <chrono>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <thread>

#include "initialize.h"
#include <derecho/sst/detail/poll_utils.hpp>
#include <derecho/sst/multicast_sst.hpp>
#include <derecho/sst/sst.hpp>

using std::cin;
using std::cout;
using std::endl;
using std::map;
using std::ofstream;
using std::string;
using std::vector;

using namespace sst;

#ifndef NDEBUG
#define DEBUG_MSG(str)                 \
    do {                               \
        std::cout << str << std::endl; \
    } while(false)
#else
#define DEBUG_MSG(str) \
    do {               \
    } while(false)
#endif

int main(int argc, char* argv[]) {
    if(argc != 5) {
        std::cout << "Usage: " << argv[0] << " <num. nodes> <num_senders> <num_msgs> <win_size> <max_msg_size>" << endl;
        return -1;
    }

    // Param section: to acquire via cmd(and make check)
    const uint32_t num_nodes = std::atoi(argv[1]);
    const uint32_t num_senders = std::atoi(argv[2]);
    const uint64_t num_msgs = std::atoll(argv[3]);
    const uint32_t window_size = std::atoi(argv[4]);
    const uint64_t max_msg_size = std::atoll(argv[5]);

    const uint32_t node_id = derecho::getConfUInt32(CONF_DERECHO_LOCAL_ID);
    const std::map<uint32_t, std::pair<ip_addr_t, uint16_t>> ip_addrs_and_ports = initialize(num_nodes);

    // Initialize the rdma resources
#ifdef USE_VERBS_API
    verbs_initialize(ip_addrs_and_ports, node_id);
#else
    lf_initialize(ip_addrs_and_ports, node_id);
#endif

    // Form a group with all the nodes
    vector<uint32_t> members;
    for(auto p : ip_addrs_and_ports) {
        members.push_back(p.first);
    }

    // Flags for senders (later, we could build a class and this could be a param)
    vector<int> is_sender(num_nodes, 0);
    for(uint32_t i = 0; i < num_senders; i++) {
        is_sender[i] = 1;
    }

    SSTParams parameters(members, node_id);

    // Create SST
    std::shared_ptr<multicast_sst> sst = std::make_shared<multicast_sst>(parameters, window_size, num_senders, max_msg_size);

    // Create a subgroup
    multicast_group subgroup<multicast_sst>(sst, members, window_size, max_msg_size, is_sender);

    // Get my rank
    const uint32_t my_rank = sst.get_local_index();

    // Initialize sst rows
    for(char& c : sst->slots[my_rank]) {
        c = -1;
    }
    for(int64_t& n : sst->num_received_sst[my_rank]) {
        n = 0;
    }
    sst->heartbeat[my_rank] = 0;
    sst->put();
    sst->sync_with_members();

    // Failures detection thread
    volatile bool shutdown = false;
    auto check_failures_loop = [&]() {
        pthread_setname_np(pthread_self(), "check_failures");
        while(!shutdown) {
            std::this_thread::sleep_for(std::chrono::microseconds(1000));
            sst.put_with_completion(sst.heartbeat);
        }
        DEBUG_MSG("Failure thread exiting ...");
    };
    std::thread failures_thread = std::thread(check_failures_loop);



    /* TODO: my logic */
    // 1. Define and register send/receive predicates following the experiment rules;
    //      Instead of calling subgroup.send(), just put the messages in the buffer and update
    //      an index of "ready" messages sst->num_received_sst[my_rank]
    // 2. Define and start two other threads:
    //      a. One that continuously checks sst->num_received_sst[my_rank] and in case of new messages, sends them
    //      b. One that continuously pushes to the other nodes the set of columns "sst->num_received_sst".



    shutdown = true;
    failures_thread.join();
    sst.sync_with_members();

    return 0;
}