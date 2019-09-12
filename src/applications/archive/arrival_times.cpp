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
    SSTFieldVector<unsigned char> msg;
    SSTField<bool> heartbeat;
};

int main(int argc, char* argv[]) {
    if(argc != 5) {
        std::cout << "Usage: " << argv[0] << " <num. nodes> <num. senders>  <number of msgs> <sender_sleep_time_ms>" << endl;
        return -1;
    }

    const uint32_t num_nodes = std::atoi(argv[1]);
    const uint32_t num_senders = std::atoi(argv[2]);
    const uint64_t num_msgs = std::atoi(argv[3]);
    const uint32_t sleep_time = std::atoi(argv[4]);

    if(num_senders > num_nodes || num_senders == 0) {
        std::cout << "Num senders must be more than zero and less or equal than num_nodes" << endl;
        return -1;
    }

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
    mySST sst(members, node_id, msg_size);

    // initalization
    for(uint i = 0; i < msg_size; i++) {
        sst.msg[node_id][i] = 0;
    }

    sst.put(sst.msg);
    sst.sync_with_members();

    auto check_failures_loop = [&sst]() {
        pthread_setname_np(pthread_self(), "check_failures");
        while(true) {
            std::this_thread::sleep_for(std::chrono::microseconds(1000));
            sst.put_with_completion(sst.heartbeat);
        }
    };

    std::thread failures_thread = std::thread(check_failures_loop);

    //sender action - just send
    auto sender = [&]() {
        pthread_setname_np(pthread_self(), "sender");

        uint64_t sent = 0;

        for(unsigned int i = 0; i < num_msgs; i++) {
            sst.msg[node_id][msg_size - 1] = (sst.msg[node_id][msg_size - 1] + 1) % (std::numeric_limits<unsigned char>::max() + 1);
            sst.put(sst.msg);
            ++sent;

            if(sleep_time > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
            }
        }
    };

    //receiver action
    auto receiver = [&](const uint32_t sender_rank) {
        pthread_setname_np(pthread_self(), ("receiver@" + std::to_string(sender_rank)).c_str());

        //index of the last received message (%256)
        uint8_t last_received = 0;
        //index of the newly received message (%256)
        uint8_t actual_received = 0;
        //index of the total number of received message (actual #)
        uint64_t highest_received = 0;

        vector<struct timespec> arrival_times(num_msgs, {0});

        while(highest_received < num_msgs) {
            actual_received = sst.msg[sender_rank][msg_size - 1];

            if(actual_received == last_received) {
                continue;
            }

            else if(actual_received > last_received) {
                highest_received += actual_received - last_received;
            }

            else {
                highest_received += 256 - last_received + actual_received;
            }

            //Here I received a new message
            clock_gettime(CLOCK_REALTIME, &arrival_times[highest_received - 1]);
            last_received = actual_received;
        }

        //print results
        double sum = 0.0;
        double time;
        uint64_t missed_msgs = 0;
        uint64_t last_valid_time = 0;
        bool already_received = false;

        ofstream fout;
        fout.open("time_records_" + std::to_string(sender_rank), ofstream::app);
        fout << "Times recorded for sender " << sender_rank << endl;
        // compute the average and print values
        // i consider only non-missed messages.

        for(uint64_t i = 0; i < num_msgs; ++i) {
            if(arrival_times[i].tv_sec == 0) {
                missed_msgs++;
                if(!already_received)
                    last_valid_time++;
            } else {
                if(!already_received)
                    already_received = true;
                else {
                    time = ((arrival_times[i].tv_sec * 1e9 + arrival_times[i].tv_nsec) - (arrival_times[last_valid_time].tv_sec * 1e9 + arrival_times[last_valid_time].tv_nsec)) / 1e9;
                    sum += time;
                    fout << time << endl;
                }
                last_valid_time = i;
            }
        }
        fout << "Average inter-arrival time (" << num_msgs << " msgs): " << (sum / (num_msgs - missed_msgs)) / 1e9 << endl;
        fout << "Missed messages: " << missed_msgs << endl;
        fout.close();
    };

    /* Receiver threads 
     * For the moment, ONE receiver thread for each sender.
     * Then, # of receiver threads per sender will be a 
     * parameter. (TODO)
    */
    vector<std::thread> receiver_threads;
    for(uint32_t i = 0; i < num_senders; i++) {
        receiver_threads.emplace_back(std::thread(receiver, i));
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
    for(auto& th : receiver_threads) {
        th.join();
        std::cout << "Receiver joined" << endl;
    }

    sst.sync_with_members();

#ifdef USE_VERBS_API
    verbs_destroy();
#else
    lf_destroy();
#endif

    return 0;
}
