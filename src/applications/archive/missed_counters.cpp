#include <chrono>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <limits>
#include <map>
#include <string>
#include <thread>

#include "initialize.h"
#include <derecho/sst/detail/poll_utils.hpp>
#include <derecho/sst/sst.hpp>

using std::cin;
using std::cout;
using std::endl;
using std::map;
using std::ofstream;
using std::string;
using std::vector;

using namespace sst;

class mySST : public SST<mySST> {
public:
    mySST(const vector<uint32_t>& members, uint32_t my_rank)
            : SST<mySST>(this, SSTParams{members, my_rank}) {
        SSTInit(counter, heartbeat);
    }
    SSTField<uint64_t> counter;
    SSTField<bool> heartbeat;
};

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

void print_average(uint32_t num_nodes, uint64_t num_msgs, uint32_t my_rank, vector<vector<uint64_t>>& received_msgs) {
    // what do we really want?
    // [sum_missed_messages_local][sum_missed_mesages_remote]

    ofstream fout("missed_results");

    uint64_t count_missed = 0;
    uint64_t last_received = (uint64_t)-1;

    //count missed local
    for(uint64_t m : received_msgs[my_rank]) {
        count_missed += m - last_received - 1;
        last_received = m;
    }

    fout << count_missed << " ";

    //count missed remote
    count_missed = 0;

    for(uint32_t i = 0; i < num_nodes; i++) {
        if(i == my_rank) {
            continue;
        }

        last_received = (uint64_t)-1;

        for(uint64_t m : received_msgs[i]) {
            count_missed += m - last_received - 1;
            last_received = m;
        }
    }

    fout << count_missed << endl;
    fout.close();
}

int main(int argc, char* argv[]) {
    if(argc != 3) {
        std::cout << "Usage: " << argv[0] << " <num. nodes> <num_msgs>" << endl;
        return -1;
    }

    const uint32_t num_nodes = std::atoi(argv[1]);
    const uint64_t num_msgs = std::atoll(argv[2]);

    if(num_nodes < 1) {
        std::cout << "Number of nodes must be at least one" << endl;
        return -1;
    }
    if(num_msgs < 1) {
        std::cout << "Number of messages must be at least one" << endl;
        return -1;
    }

    const uint32_t node_id = derecho::getConfUInt32(CONF_DERECHO_LOCAL_ID);
    const std::map<uint32_t, std::pair<ip_addr_t, uint16_t>> ip_addrs_and_ports = initialize(num_nodes);

    // initialize the rdma resources
#ifdef USE_VERBS_API
    verbs_initialize(ip_addrs_and_ports, node_id);
#else
    lf_initialize(ip_addrs_and_ports, node_id);
#endif

    // form a group with all the nodes
    // all nodes will send and receive
    vector<uint32_t> members;
    for(auto p : ip_addrs_and_ports) {
        members.push_back(p.first);
    }

    // create a new shared state table with all the members
    mySST sst(members, node_id);

    // get my rank
    const uint32_t my_rank = sst.get_local_index();

    // initialize the counter
    sst.counter[my_rank] = 0;
    sst.heartbeat[my_rank] = 0;
    sst.put();
    sst.sync_with_members();

    // failures detection thread
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

    auto sender_loop = [&]() {
        pthread_setname_np(pthread_self(), "sender");
        DEBUG_MSG("Sender started");

        for(uint64_t i = 0; i < num_msgs; i++) {
            sst.counter[my_rank]++;
            sst.put(sst.counter);
        }

        DEBUG_MSG("Sender finished");
    };

    //vector of received messages
    vector<vector<uint64_t>> received_msgs(num_nodes, vector<uint64_t>());

    auto receiver_loop = [&]() {
        pthread_setname_np(pthread_self(), "receiver");
        DEBUG_MSG("Received started");

        //index of the last received message
        vector<uint64_t> last_received(num_nodes, (uint64_t)-1);
        //index of the newly received message
        vector<uint64_t> actual_received(num_nodes, 0);

        while(std::all_of(actual_received.begin(), actual_received.end(), [&](int n) { return n < (int)num_msgs; })) {
            for(uint32_t i = 0; i < num_nodes; i++) {
                actual_received[i] = (uint64_t&)sst.counter[i];

                if(actual_received[i] == last_received[i]) {
                    continue;
                }
                received_msgs[i].push_back(actual_received[i]);
                last_received = actual_received;
            }
        }
        DEBUG_MSG("Receiver finished");
    };

    /* Receiver thread */
    std::thread receiver_thread(receiver_loop);
    sst.sync_with_members();

    /* Sender thread */
    std::thread sender_thread(sender_loop);

    // wait for threads to join
    sender_thread.join();
    receiver_thread.join();

    shutdown = true;
    failures_thread.join();
    sst.sync_with_members();

    // print results
    print_average(num_nodes, num_msgs, my_rank, received_msgs);
    
    return 0;
}
