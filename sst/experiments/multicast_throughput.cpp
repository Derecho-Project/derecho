#include <chrono>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include "derecho/experiments/aggregate_bandwidth.h"
#include "derecho/experiments/log_results.h"
#include "sst/multicast.h"
#include "sst/multicast_sst.h"

using namespace std;
using namespace sst;

volatile bool done = false;

struct exp_results {
  uint32_t num_nodes;
  int num_senders_selector;
  uint max_msg_size;
  double sum_message_rate;
  void print(std::ofstream& fout) {
      fout << num_nodes << " " << num_senders_selector << " "
           << max_msg_size << " " << sum_message_rate << endl;
  }
};

int main(int argc, char* argv[]) {
    constexpr uint max_msg_size = 1, window_size = 1000;
    const unsigned int num_messages = 1000000;
    int num_senders_selector = atoi(argv[1]);
    // input number of nodes and the local node id
    uint32_t node_id, num_nodes;
    cin >> node_id >> num_nodes;

    // input the ip addresses
    map<uint32_t, string> ip_addrs;
    for(unsigned int i = 0; i < num_nodes; ++i) {
        cin >> ip_addrs[i];
    }

    // initialize the rdma resources
    verbs_initialize(ip_addrs, node_id);

    std::vector<uint32_t> members(num_nodes);
    for(uint i = 0; i < num_nodes; ++i) {
        members[i] = i;
    }

    std::shared_ptr<multicast_sst> sst = make_shared<multicast_sst>(
            sst::SSTParams(members, node_id),
            window_size);

    auto check_failures_loop = [&sst]() {
        pthread_setname_np(pthread_self(), "check_failures");
        while(true) {
            std::this_thread::sleep_for(chrono::microseconds(100));
            if(sst) {
                sst->put_with_completion((char*)std::addressof(sst->heartbeat[0]) - sst->getBaseAddress(), sizeof(bool));
            }
        }
    };

    thread failures_thread = std::thread(check_failures_loop);

    uint num_finished = 0;
    auto sst_receive_handler = [&num_finished, num_senders_selector, &num_nodes, &num_messages](
            uint32_t sender_rank, uint64_t index,
            volatile char* msg, uint32_t size) {
        if(index == num_messages - 1) {
            num_finished++;
        }
        if(num_finished == num_nodes || (num_senders_selector == 1 && num_finished == num_nodes / 2) || (num_senders_selector == 2 && num_finished == 1)) {
            done = true;
        }
    };
    auto receiver_pred = [](const multicast_sst&) {
        return true;
    };
    auto num_times = window_size / num_nodes;
    if(!num_times) {
        num_times = 1;
    }
    auto receiver_trig = [num_times, num_nodes, node_id, sst_receive_handler](multicast_sst& sst) {
        bool update_sst = false;
        for(uint i = 0; i < num_times; ++i) {
            for(uint j = 0; j < num_nodes; ++j) {
                uint32_t slot = sst.num_received_sst[node_id][j] % window_size;
                if((int64_t)sst.slots[j][slot].next_seq == (sst.num_received_sst[node_id][j]) / window_size + 1) {
                    sst_receive_handler(j, sst.num_received_sst[node_id][j],
                                        sst.slots[j][slot].buf,
                                        sst.slots[j][slot].size);
                    sst.num_received_sst[node_id][j]++;
                    update_sst = true;
                }
            }
        }
        if(update_sst) {
            sst.put(sst.num_received_sst.get_base() - sst.getBaseAddress(),
                    sizeof(sst.num_received_sst[0][0]) * num_nodes);
        }
    };
    sst->predicates.insert(receiver_pred, receiver_trig,
                           sst::PredicateType::RECURRENT);

    struct timespec start_time, end_time;
    vector<uint32_t> indices;
    iota(indices.begin(), indices.end(), 0);
    sst::multicast_group<multicast_sst> g(sst, indices, window_size);
    // uint count = 0;
    // start timer
    clock_gettime(CLOCK_REALTIME, &start_time);
    if(node_id == 0 || num_senders_selector == 0 || (node_id > (num_nodes - 1) / 2 && num_senders_selector == 1)) {
        for(uint i = 0; i < num_messages; ++i) {
            volatile char* buf;
            while((buf = g.get_buffer(max_msg_size)) == NULL) {
                // ++count;
            }
            // for(uint i = 0; i < size; ++i) {
            //     buf[i] = 'a' + rand() % 26;
            // }
            g.send();
        }
    }
    // cout << "Done sending" << endl;
    while(!done) {
    }
    // end timer
    clock_gettime(CLOCK_REALTIME, &end_time);
    double my_time = ((end_time.tv_sec * 1e9 + end_time.tv_nsec) - (start_time.tv_sec * 1e9 + start_time.tv_nsec));
    double message_rate = (num_messages * 1e9) / my_time;
    ;
    if(num_senders_selector == 0) {
        message_rate *= num_nodes;
    } else if(num_senders_selector == 1) {
        message_rate *= num_nodes / 2;
    }

    // cout << "Time in nanoseconds " << my_time << endl;
    // cout << "Number of messages per second " << message_rate
    //      << endl;
    // cout << "Number of times null returned, count = " << count << endl;
    double sum_message_rate = aggregate_bandwidth(members, node_id, message_rate);
    log_results(exp_results{num_nodes, num_senders_selector, max_msg_size, sum_message_rate},
		"data_multicast");
    sst->sync_with_members();
}
