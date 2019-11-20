#include <iostream>
#include <fstream>
#include <vector>

#include "initialize.h"
#include <derecho/sst/detail/poll_utils.hpp>
#include <derecho/sst/sst.hpp>

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

class mySST : public SST<mySST> {
public:
    mySST(const std::vector<uint32_t>& members, uint32_t my_rank)
            : SST<mySST>(this, SSTParams{members, my_rank}) {
        SSTInit(counter, heartbeat);
    }
    SSTField<uint64_t> counter;
    SSTField<bool> heartbeat;
};

std::pair<uint64_t, uint64_t> compute_missed(const std::vector<bool>& received,
                                             const uint32_t start, const uint32_t end) {
    std::pair<uint64_t, uint64_t> counts{0, 0};
    for (uint32_t i = start; i <= end; ++i) {
        if (!received[i]) {
	    counts.first++;
	    if (i == 0 || received[i-1]) {
	        counts.second++;
	    }
	}
    }
    return counts;
}

void print_stats(const uint32_t num_nodes, const uint64_t num_msgs, const uint32_t my_rank,
		 const std::vector<std::vector<bool>>& received_msgs) {
    // consider messages with index s.t. num_msgs / 2 + 1 <= index <= num_msgs

    uint64_t local_missed_counters = 0;
    uint64_t local_missed_intervals = 0;
    uint64_t remote_missed_counters = 0;
    uint64_t remote_missed_intervals = 0;
    
    for (uint32_t i = 0; i < num_nodes; ++i) {
        std::pair<uint64_t, uint64_t> p = compute_missed(received_msgs[i], num_msgs / 2 + 1, num_msgs);
        if (i == my_rank) {
	    local_missed_counters = p.first;
	    local_missed_intervals = p.second;
        } else {
            remote_missed_counters += p.first;
	    remote_missed_counters += p.second;
        }
    }
    
    std::ofstream fout("missed_results");

    // local
    if (local_missed_intervals == 0) {
        fout << "0 0 ";
    } else {
        fout << local_missed_counters / local_missed_intervals
             << " " << local_missed_intervals << " ";
    }

    // remote
    if (remote_missed_intervals == 0) {
        fout << "0 0 ";
    } else {
        fout << remote_missed_counters / remote_missed_intervals
             << " " << remote_missed_intervals << " ";
    }

    fout << std::endl;
}

int main(int argc, char* argv[]) {
    if(argc != 3) {
        std::cout << "Usage: " << argv[0] << " <num. nodes> <num_msgs>" << std::endl;
        return 1;
    }

    const uint32_t num_nodes = std::atoi(argv[1]);
    const uint64_t num_msgs = std::atoll(argv[2]);

    if(num_nodes < 1) {
      std::cout << "Number of nodes must be at least one" << std::endl;
      return 1;
    }
    if(num_msgs < 1) {
      std::cout << "Number of messages must be at least one" << std::endl;
      return 1;
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
    std::vector<uint32_t> members;
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
    
    std::vector<std::vector<bool>> received_msgs(num_nodes, std::vector<bool>(num_msgs + 1, false));

    auto receiver_loop = [&]() {
        pthread_setname_np(pthread_self(), "receiver");
        DEBUG_MSG("Received started");

        bool done = false;
        while(!done) {
	    done = true;
            for(uint32_t i = 0; i < num_nodes; ++i) {
                if(received_msgs[i].back()) {
                    continue;
                }
		done = false;
                received_msgs[i][sst.counter[i]] = true;
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

    print_stats(num_nodes, num_msgs, my_rank, received_msgs);

    shutdown = true;
    failures_thread.join();
    sst.sync_with_members();
}
