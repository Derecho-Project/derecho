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
        SSTInit(finished);
    }
    SSTField<bool> finished;
};

int main(int argc, char* argv[]) {
    if(argc != 4) {
        std::cout << "Usage: " << argv[0] << " <num. nodes> <num_msgs> <mode>" << std::endl;
        std::cout << "Mode: 0 all senders - 1 half senders - 2 single sender" << std::endl;
        return 1;
    }

    const uint32_t num_nodes = std::atoi(argv[1]);
    const uint64_t num_msgs = std::atoll(argv[2]);
    const uint8_t mode = std::atoi(argv[3]);

    if(num_nodes < 1) {
      std::cout << "Number of nodes must be at least one" << std::endl;
      return 1;
    }
    if(num_msgs < 1) {
      std::cout << "Number of messages must be at least one" << std::endl;
      return 1;
    }
    if(mode < 0 || mode > 2) {
      std::cout << "Mode: 0 all senders - 1 half senders - 2 single sender" << std::endl;
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
    std::vector<uint32_t> members;
    for(auto p : ip_addrs_and_ports) {
        members.push_back(p.first);
    }

    // create a new shared state table with all the members
    mySST sst(members, node_id);

    // get my rank
    const uint32_t my_rank = sst.get_local_index();

    bool i_am_sender = false;
    if(mode == 0 || (mode == 1 && my_rank < num_nodes/2) || (mode == 2 && my_rank == 0)) {
        i_am_sender = true;
    }


    // initialize sst
    sst.finished[my_rank] = false;
    sst.put();
    sst.sync_with_members();

    
    struct timespec start_time, end_time;
    auto sender_loop = [&]() {
        pthread_setname_np(pthread_self(), "sender");
        DEBUG_MSG("Sender started");

        // start timer
        clock_gettime(CLOCK_REALTIME, &start_time);
        for(uint64_t i = 0; i < num_msgs; i++) {
            sst.put();
        }
        // end timer
        clock_gettime(CLOCK_REALTIME, &end_time);

        //send message about completion
        sst.finished[my_rank] = true;
        sst.put();

        DEBUG_MSG("Sender finished");
    };
    
    auto receiver_loop = [&]() {
        pthread_setname_np(pthread_self(), "receiver");
        DEBUG_MSG("Received started");

        bool done = false;
        while(!done) {
	        done = true;
            for(uint32_t i = 0; i < num_nodes; ++i) {
                if(sst.finished[i]) {
                    continue;
                }
        		done = false;
            }
        }
        DEBUG_MSG("Receiver finished");
    };

    /* Receiver thread */
    std::thread receiver_thread(receiver_loop);
    sst.sync_with_members();

    if(i_am_sender) {
        /* Sender thread */
        std::thread sender_thread(sender_loop);
        sender_thread.join();
    }
    receiver_thread.join();

    //print results
    if(i_am_sender) {
        double my_time = ((end_time.tv_sec * 1e9 + end_time.tv_nsec) - (start_time.tv_sec * 1e9 + start_time.tv_nsec));
        double message_rate = (num_msgs * 1e9) / my_time;
        std::cout << num_msgs << " msgs in " << my_time << " s => (" << message_rate << " msgs/s)" << std::endl; 
    }

    sst.sync_with_members();
}
