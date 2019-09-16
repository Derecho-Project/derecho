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
//Since all SST instances are named sst, we can use this convenient hack
#define LOCAL sst.get_local_index()

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
    mySST(const vector<uint32_t>& _members, uint32_t my_rank, uint64_t _msg_size)
            : SST<mySST>(this, SSTParams{_members, my_rank}),
              msg(_msg_size) {
        SSTInit(msg, heartbeat);
    }
    SSTFieldVector<uint64_t> msg;
    SSTField<bool> heartbeat;
};

int main(int argc, char* argv[]) {
    if(argc < 6 || argc > 7) {
        std::cout << "Usage: " << argv[0] << " <num. nodes> <num. senders> <num. msgs> <sender_sleep_time_ms> <window size>" << endl;
        return -1;
    }

    const uint32_t num_nodes = std::atoi(argv[1]);
    const uint32_t num_senders = std::atoi(argv[2]);
    uint64_t num_msgs = std::atoi(argv[3]); 
    const uint32_t sleep_time = std::atoi(argv[4]);
    const uint32_t window_size = std::atoi(argv[5]);  //Now window size is an input param - later we could use the derecho.cfg file
    
    if(num_senders > num_nodes || num_senders == 0) {
        std::cout << "Num senders must be more than zero and less or equal than num_nodes" << endl;
        return -1;
    }

    // Make num_msg a multiple of window size
    num_msgs += (num_msgs % window_size)==0? 0 : window_size - (num_msgs % window_size);

    const uint32_t node_id = derecho::getConfUInt32(CONF_DERECHO_LOCAL_ID);
    const uint64_t msg_size = derecho::getConfUInt64(CONF_SUBGROUP_DEFAULT_MAX_SMC_PAYLOAD_SIZE);
    const std::map<uint32_t, std::pair<ip_addr_t, uint16_t>> ip_addrs_and_ports = initialize(num_nodes);

    // initialize the rdma resources
#ifdef USE_VERBS_API
    verbs_initialize(ip_addrs_and_ports, node_id);
#else
    lf_initialize(ip_addrs_and_ports, node_id);
#endif

    // form a group with all the nodes
    vector<uint32_t> members;
    for(auto p : ip_addrs_and_ports) {
        members.push_back(p.first);
    }

    //form a subset of senders
    vector<uint32_t> senders(members.begin(), members.begin() + num_senders);

    // create a new shared state table with all the members
    mySST sst(members, node_id, msg_size * window_size);

    // initalization
    for(uint i = 0; i < msg_size * window_size; i++) {
        sst.msg[node_id][i] = 0;
    }
    sst.put(sst.msg);
    sst.sync_with_members();

    volatile bool shutdown = false;
    auto check_failures_loop = [&]() {
        pthread_setname_np(pthread_self(), "check_failures");
        while(!shutdown) {
            std::this_thread::sleep_for(std::chrono::microseconds(1000));
            sst.put_with_completion(sst.heartbeat);
        }
        std::cout << "Failure thread exiting ..." << endl;
    };

    std::thread failures_thread = std::thread(check_failures_loop);

    //sender action - just send
    auto sender = [&]() {
        pthread_setname_np(pthread_self(), "sender");

        //Debug
        std::cout << "Sender started" << endl;

        uint64_t sent = 0;

        for(unsigned int i = 0; i < num_msgs; i++) {
            // Slot to use: (i % window_size) * msg_size;
            // I compute the nex one and subtract sizeof(uint64_t)
            sst.msg[node_id][(i % window_size + 1) * msg_size - 1] = sent + 1;

            /* Check here the message size, which is actually 
             * msg_size * sizeof(uint64_t)
             */

            sst.put((char*)std::addressof(sst.msg[0][(i % window_size) * msg_size]) - sst.getBaseAddress(), msg_size * sizeof(uint64_t));
            ++sent;

            if(sleep_time > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
            }
        }
    };

    vector<vector<struct timespec>> arrival_times(num_senders, vector<struct timespec>(num_msgs, {0}));
    
    //receiver action
    auto receiver = [&](const uint32_t sender_rank, const uint32_t slot) {
        pthread_setname_np(pthread_self(), ("receiver" + std::to_string(slot) + "@" + std::to_string(sender_rank)).c_str());

        //indexes of the last received message
        uint64_t last_received = 0;
        //indexes of the newly received message
        uint64_t actual_received = 0;
        //total to be received
        uint64_t max_msg_index = num_msgs - window_size + 1 + slot ;
        //index_of_msg
        uint64_t index_of_msg = (slot + 1) * msg_size - 1;

        while(actual_received < max_msg_index) {

            actual_received = sst.msg[sender_rank][index_of_msg];

            if(actual_received == last_received) {
                continue;
            }
        
            //Here I received a new message
            clock_gettime(CLOCK_REALTIME, &arrival_times[sender_rank][actual_received - 1]);
            last_received = actual_received;
        }
    };

    /* Receiver threads 
     * One thread per slot per receiver
     */
    vector<vector<std::thread>> receiver_threads(num_nodes);

    for(uint32_t i = 0; i < num_senders; i++) {
        receiver_threads[i] = vector<std::thread>();
        for(uint32_t j = 0; j < window_size; j++) {
            receiver_threads[i].emplace_back(std::thread(receiver, i, j));
        }
    }

    uint32_t my_rank = sst.get_local_index();
    // Sender thread, if local node is a sender.
    // Creates the thread and waits for termination
    if(my_rank < num_senders) {
        //thread creation
        std::thread sender_thread(sender);

        //wait for thread termination
        sender_thread.join();
    }

    //Wait for the receivers
    for(uint32_t i = 0; i < num_senders; i++) {
        for(uint32_t j = 0; j < window_size; j++) {
            receiver_threads[i][j].join();
            std::cout << "Thread " << j << " for receiver " << i << " joined" << endl;
        }
    }

    //Print results in the format: [msg_index] [arrival time]
    for(unsigned int i = 0; i < num_senders; i++) {
        //vector to keep track of which message I missed
        vector<uint64_t> missed_msgs;
        ofstream fout;
        fout.open("time_records_" + std::to_string(i));
        fout << "Times recorded for sender " << i << endl;
        for(uint64_t j = 0; j < num_msgs; j++) {
            if(arrival_times[i][j].tv_sec == 0) {
                missed_msgs.push_back(j + 1);
            } else {
                uint64_t time = arrival_times[i][j].tv_sec * (uint64_t)1e9 + arrival_times[i][j].tv_nsec;
                fout << (j + 1) << " " << time << endl;
            }
        }
        fout << "Missed messages: " << missed_msgs.size() << endl;
        // If we need to print which are the missed messages
        // for(const auto& m : missed_msgs)
        //     fout << (int)m << endl;
        fout.close();
    }

    shutdown = true;
    failures_thread.join();

    sst.sync_with_members();

    return 0;
}
