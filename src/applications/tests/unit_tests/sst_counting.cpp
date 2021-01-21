/**
 * @file sst_counting.cpp
 *
 * This file demonstrates basic SST functionality by constructing a simple SST
 * with one field, an integer counter. The participating nodes should only
 * increment their local counter to i when every other node has incremented its
 * counter to at least i-1.
 */

#include <chrono>
#include <iostream>
#include <map>
#include <thread>
#include <vector>

#include <derecho/sst/sst.hpp>

#include "initialize.hpp"

using namespace sst;

class mySST : public SST<mySST> {
public:
    mySST(const std::vector<uint32_t>& _members, uint32_t my_rank) : SST<mySST>(this, SSTParams{_members, my_rank}) {
        SSTInit(counter, heartbeat);
    }
    SSTField<int> counter;
    SSTField<bool> heartbeat;
};

int main(int argc, char* argv[]) {
    const int END_VALUE = 1000000;
    if(argc < 2) {
        std::cout << "Argument required: num_nodes" << std::endl;
        std::exit(1);
    }
    uint32_t num_nodes = atoi(argv[1]);
    //Read local ID from config file
    const uint32_t node_id = derecho::getConfUInt32(CONF_DERECHO_LOCAL_ID);
    //Contact the other nodes to receive their IDs and IP addresses
    const std::map<uint32_t, std::pair<ip_addr_t, uint16_t>> ip_addrs_and_ports = initialize(num_nodes);
    // initialize the rdma resources
#ifdef USE_VERBS_API
    verbs_initialize(ip_addrs_and_ports, {}, node_id);
#else
    lf_initialize(ip_addrs_and_ports, {}, node_id);
#endif

    std::vector<uint32_t> members;
    for(const auto& p : ip_addrs_and_ports) {
        members.push_back(p.first);
    }

    // create a new shared state table with all the members
    mySST sst(members, node_id);
    uint32_t local_row = sst.get_local_index();
    //Initialize the local row
    sst.counter[local_row] = 0;
    sst.put(sst.counter);

    // Set up the failure-checking thread, in the same way Derecho would
    auto check_failures_loop = [&sst]() {
        pthread_setname_np(pthread_self(), "check_failures");
        while(true) {
            std::this_thread::sleep_for(std::chrono::microseconds(1000));
            sst.put_with_completion((char*)std::addressof(sst.heartbeat[0]) - sst.getBaseAddress(), sizeof(bool));
        }
    };

    std::thread failures_thread = std::thread(check_failures_loop);

    bool ready_to_start = false;
    // wait till all counters are 0
    while(ready_to_start == false) {
        ready_to_start = true;
        for(unsigned int i = 0; i < num_nodes; ++i) {
            if(sst.counter[i] != 0) {
                ready_to_start = false;
            }
        }
    }

    //Do a TCP handshake with all members before starting
    for(unsigned int i = 0; i < num_nodes; ++i) {
        if(i == local_row) {
            continue;
        }
        sync(i);
    }

    struct timespec start_time;

    // the predicate: checks whether all other rows have a value >= the value in my row
    auto counter_pred = [num_nodes](const mySST& sst) {
        for(unsigned int i = 0; i < num_nodes; ++i) {
            if(sst.counter[i] < sst.counter[sst.get_local_index()]) {
                return false;
            }
        }
        return true;
    };

    // the trigger: increments my value
    auto counter_trig = [&start_time](mySST& sst) {
        uint32_t local_row = sst.get_local_index();
        ++(sst.counter[local_row]);
        sst.put(sst.counter);
        //Check to see if the test is over
        if(sst.counter[local_row] == END_VALUE) {
            // end timer
            struct timespec end_time;
            clock_gettime(CLOCK_REALTIME, &end_time);
            // my_time is time taken to count
            double my_time = ((end_time.tv_sec * 1e9 + end_time.tv_nsec) - (start_time.tv_sec * 1e9 + start_time.tv_nsec)) / 1e9;
            int num_nodes = sst.get_num_rows();
            std::cout << num_nodes << " " << my_time << std::endl;
            if(local_row == 0) {
                // sync to tell other nodes to exit
                for(int i = 0; i < num_nodes; ++i) {
                    if(i == local_row) {
                        continue;
                    }
                    sync(i);
                }
            } else {
                //sync with node 0
                sync(0);
            }
#ifdef USE_VERBS_API
            verbs_destroy();
#else
            lf_destroy();
#endif
            exit(0);
        }
    };

    // start timer
    clock_gettime(CLOCK_REALTIME, &start_time);

    // register as a recurring predicate (evaluation starts immediately)
    sst.predicates.insert(counter_pred, counter_trig, PredicateType::RECURRENT);

    while(true) {
    }
    return 0;
}
