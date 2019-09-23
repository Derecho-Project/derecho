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
    mySST(const vector<uint32_t>& members, uint32_t my_rank, uint64_t buffer_size)
            : SST<mySST>(this, SSTParams{members, my_rank}),
              buffer(buffer_size) {
        SSTInit(buffer, heartbeat);
    }
    SSTFieldVector<char> buffer;
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

int main(int argc, char* argv[]) {
    if(argc != 8) {
        std::cout << "Usage: " << argv[0] << " <num. nodes> <num. senders> <num. msgs> <msg size> <sender_sleep_time_ms> <window size> <num_thread_per_recv>" << endl;
        return -1;
    }

    const uint32_t num_nodes = std::atoi(argv[1]);
    const uint32_t num_senders = std::atoi(argv[2]);
    uint64_t num_msgs = std::atoll(argv[3]);
    const uint32_t msg_size = std::atoi(argv[4]);
    const uint32_t sleep_time = std::atoi(argv[5]);
    // window_size is an input param - not to be confused with window_size from the derecho.cfg file
    const uint32_t window_size = std::atoi(argv[6]);
    const uint32_t num_thread_per_recv = std::atoi(argv[7]);

    if(num_senders <= 0 || num_senders > num_nodes) {
        std::cout << "Num senders must be more than zero and less or equal than num_nodes" << endl;
        return -1;
    }

    if(num_thread_per_recv <= 0 || num_thread_per_recv > window_size) {
        std::cout << "Num threads per recv must be more than zero and less or equal than window size" << endl;
        return -1;
    }

    // Make num_msgs a multiple of window size
    num_msgs = ((num_msgs - 1) / window_size + 1) * window_size;

    const uint32_t node_id = derecho::getConfUInt32(CONF_DERECHO_LOCAL_ID);
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
    uint32_t my_rank = sst.get_local_index();

    // initalization
    for(uint i = 0; i < sst.buffer.size(); i++) {
        sst.buffer[my_rank][i] = 0;
    }
    sst.put(sst.buffer);
    sst.sync_with_members();

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

    vector<struct timespec> send_times(num_msgs, {0});
    //sender action - just send
    auto sender_loop = [&]() {
        pthread_setname_np(pthread_self(), "sender");
        DEBUG_MSG("Sender started");

        for(uint64_t msg_num = 0; msg_num < num_msgs; msg_num++) {
            const uint64_t slot_offset = (msg_num % window_size) * msg_size;
            // I compute the next one and subtract sizeof(uint64_t)
            (uint64_t&)sst.buffer[my_rank][slot_offset + msg_size - sizeof(uint64_t)] = msg_num + 1;
            sst.put((char*)std::addressof(sst.buffer[0][slot_offset]) - sst.getBaseAddress(), msg_size);
            clock_gettime(CLOCK_REALTIME, &send_times[msg_num]);

            if(sleep_time > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
            }
        }
    };

    vector<vector<struct timespec>> arrival_times(num_senders, vector<struct timespec>(num_msgs, {0}));
    //receiver action
    auto receiver_loop = [&](const uint32_t sender_rank, const uint32_t slot) {
        pthread_setname_np(pthread_self(), ("receiver" + std::to_string(slot) + "@" + std::to_string(sender_rank)).c_str());

        //index of the last received message
        uint64_t last_received = 0;
        //index of the newly received message
        uint64_t actual_received = 0;
        //total to be received
        uint64_t max_msg_index = num_msgs - window_size + 1 + slot;
        //index_of_msg
        uint64_t index_of_msg = (slot + 1) * msg_size - sizeof(uint64_t);

        while(actual_received < max_msg_index) {
            actual_received = (uint64_t&)sst.buffer[sender_rank][index_of_msg];
            if(actual_received == last_received) {
                continue;
            }

            //Here I received a new message
            clock_gettime(CLOCK_REALTIME, &arrival_times[sender_rank][actual_received - 1]);
            last_received = actual_received;
        }
    };

    /* Receiver threads 
     * One thread per slot per receiver if thread_per_recv == window_size
     * Otherwise, we monitor only the first thread_per_recv slots
     */
    vector<vector<std::thread>> receiver_threads(num_nodes);
    for(uint32_t i = 0; i < num_senders; i++) {
        receiver_threads[i] = vector<std::thread>();
        for(uint32_t j = 0; j < num_thread_per_recv; j++) {
            receiver_threads[i].emplace_back(std::thread(receiver_loop, i, j));
        }
    }

    // make sure all receiver threads have started at all nodes
    // before starting the sender thread
    sst.sync_with_members();

    // Sender thread, if local node is a sender.
    if(my_rank < num_senders) {
        //thread creation
        std::thread sender_thread(sender_loop);
        //wait for thread termination
        sender_thread.join();
    }

    //Wait for the receivers
    for(uint32_t i = 0; i < num_senders; i++) {
        for(uint32_t j = 0; j < num_thread_per_recv; j++) {
            receiver_threads[i][j].join();
            DEBUG_MSG("Thread " << j << " for receiver " << i << " joined");
        }
    }

    {
        //Print only the second half of the messages
        uint64_t last_valid_time = send_times[num_msgs / 2 - 1].tv_sec * (uint64_t)1e9 + send_times[num_msgs / 2 - 1].tv_nsec;
        ofstream fout("send_times");
        for(uint i = num_msgs / 2; i < num_msgs; i++) {
            uint64_t time = send_times[i].tv_sec * (uint64_t)1e9 + send_times[i].tv_nsec;
            fout << i << " " << time % (uint64_t)1e9 << " " << (int)(time - last_valid_time) << endl;
            last_valid_time = time;
        }
    }

    //Print results in the format: [msg_index] [arrival time] [diff from previous time]
    for(uint i = 0; i < num_senders; i++) {
        //vector to keep track of which message I missed
        vector<uint64_t> missed_msgs;
        ofstream fout("receive_times_" + std::to_string(i));
        //fout << "Times recorded for sender " << i << endl;
        //Print only the second half of the messages
        uint64_t last_valid_time = arrival_times[i][num_msgs / 2 - 1 - (window_size - num_thread_per_recv)].tv_sec * (uint64_t)1e9 + arrival_times[i][num_msgs / 2 - 1 - (window_size - num_thread_per_recv)].tv_nsec;
        //This loop to avoid the first interval to be huge
        for(unsigned int i = 2 + (window_size - num_thread_per_recv); last_valid_time == 0; i+= 1+(window_size - num_thread_per_recv)) {
            last_valid_time = arrival_times[i][num_msgs / 2 - i].tv_sec * (uint64_t)1e9 + arrival_times[i][num_msgs / 2 - i].tv_nsec;
        }
        for(uint64_t j = num_msgs / 2; j < num_msgs; j++) {
            if(arrival_times[i][j].tv_sec == 0) {
                missed_msgs.push_back(j + 1);
            } else {
                uint64_t time = arrival_times[i][j].tv_sec * (uint64_t)1e9 + arrival_times[i][j].tv_nsec;
                fout << j << " " << time % (uint64_t)1e9 << " " << (int)(time - last_valid_time) << endl;
                last_valid_time = time;
            }
        }
        //fout << "Missed messages: " << missed_msgs.size() << endl;
    }

    shutdown = true;
    failures_thread.join();
    sst.sync_with_members();

    return 0;
}
