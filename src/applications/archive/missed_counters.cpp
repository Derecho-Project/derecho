#include <bitset>
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

void print_binary(uint64_t num) {
    std::bitset<16> bin(num);
    for (int i = 0; i < 4; ++i) {
      for (int j = 0; j < 4; ++j) {
          std::cout << bin[15 - i * 4 - j];
      }
      std::cout << " ";
    }
    std::cout << std::endl;
}

void print_partial_sums(uint32_t num_nodes, uint64_t num_msgs, uint32_t my_rank, vector<vector<uint64_t>>& received_msgs) {
    // what do we really want?
    // [sum_missed_messages_local][num_intervals_with_missed][sum_missed_mesages_remote][num_intervals_with_missed]

    ofstream fout("missed_results");

    uint64_t count_missed = 0;
    uint64_t num_intervals_with_missed = 0;

    // count local

    // get logic size of the vector:
    uint64_t logic_size = 1;
    for(uint64_t i = 1; i < received_msgs[my_rank].size(); i++) {
        if(received_msgs[my_rank][i] > received_msgs[my_rank][i - 1]) {
            logic_size++;
        } else {
             break;
        }
    }
    // get start index (> num_msgs/2)
    uint64_t start_index = 0;
    for(uint64_t i = 0; i < logic_size; i++) {
        if(received_msgs[my_rank][i] > num_msgs / 2) {
            start_index = i;
            break;
        }
    }

    // actual count - here I change the vector, as I'm
    // interested in the SECOND HALF of the messages
    received_msgs[my_rank][start_index - 1] = num_msgs / 2;
    for(uint64_t j = start_index; j < logic_size; j++) {
        count_missed += received_msgs[my_rank][j] - received_msgs[my_rank][j - 1] - 1;
        if(received_msgs[my_rank][j] - received_msgs[my_rank][j - 1] - 1 > 0) {
            num_intervals_with_missed++;
        }
    }

    fout << count_missed << " " << num_intervals_with_missed << " ";

    // count missed remote
    count_missed = 0;
    num_intervals_with_missed = 0;

    for(uint32_t i = 0; i < num_nodes; i++) {
        if(i == my_rank) {
            continue;
        }

        // get logic size of the vector
        logic_size = 1;
        for(uint64_t j = 1; j < received_msgs[i].size(); j++) {
            if(received_msgs[i][j] > received_msgs[i][j - 1]) {
                logic_size++;
            } else {
                break;
            }
        }

        // get start index (> num_msgs/2)
        start_index = 0;
        for(uint64_t j = 0; j < logic_size; j++) {
            if(received_msgs[i][j] > num_msgs / 2 ) {
                start_index = j;
                break;
            }
        }

        // actual count - here I change the vector, as I'm
        // interested in the SECOND HALF of the messages
        received_msgs[i][start_index - 1] = num_msgs / 2;  //<--- if start index is 0? Unlikely but would be a problem
                                                           // ACTUALLY IT IS A PROBLEM in case of non monotonicity...
        for(uint64_t j = start_index; j < logic_size; j++) {
            count_missed += received_msgs[i][j] - received_msgs[i][j - 1] - 1;
            if(received_msgs[i][j] - received_msgs[i][j - 1] - 1 > 0) {
                num_intervals_with_missed++;
            }
        }
    }
    fout << count_missed << " " << num_intervals_with_missed << endl;
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
    // all will send and receive
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
    vector<vector<uint64_t>> received_msgs(num_nodes, vector<uint64_t>(num_msgs, 0));

    auto receiver_loop = [&]() {
        pthread_setname_np(pthread_self(), "receiver");
        DEBUG_MSG("Received started");

        //index of the last received message
        vector<uint64_t> last_received(num_nodes, 0);
        //index of the newly received message
        uint64_t actual_received = 0;
        //vector of indexes
        vector<uint64_t> j(num_nodes, 0);

        while(!std::all_of(last_received.begin(), last_received.end(), [&](uint64_t n) { return n == num_msgs; })) {
            for(uint32_t i = 0; i < num_nodes; i++) {
                actual_received = sst.counter[i];
                if(actual_received == last_received[i]) {
                    continue;
                }
                received_msgs[i][j[i]] = actual_received;
                j[i]++;
                last_received[i] = actual_received;
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

    // This part shows the problem of non-monotonicity,
    // that sometimes appears together with a bad error
    // on program exit...
    for(uint32_t i = 0; i < num_nodes; i++) {
        for(uint j = 1; j < num_msgs; j++) {
            if(received_msgs[i][j] < received_msgs[i][j-1] && received_msgs[i][j]!= 0) {
                std::cout << "ERROR!! from node_ranked " << i << ", id " << members[i] << endl;
		print_binary(received_msgs[i][j-1]);
		print_binary(received_msgs[i][j]);
		print_binary(received_msgs[i][j+1]);
            }   
        }
    }

    while(true) {

    }

    // // print results
    // print_partial_sums(num_nodes, num_msgs, my_rank, received_msgs);

    return 0;
}
