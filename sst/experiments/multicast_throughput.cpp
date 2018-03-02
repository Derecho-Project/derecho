#include <chrono>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include "derecho/experiments/aggregate_bandwidth.h"
#include "derecho/experiments/log_results.h"
#include "sst/max_msg_size.h"
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
    assert(max_msg_size == 1);
    constexpr uint max_msg_size = 1, window_size = 1000;
    const unsigned int num_messages = 1000000;
    if(argc < 2) {
        cout << "Insufficient number of command line arguments" << endl;
        cout << "Enter num_senders" << endl;
        cout << "Thank you" << endl;
        exit(1);
    }
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
#ifdef USE_VERBS_API
    verbs_initialize(ip_addrs, node_id);
#else
    lf_initialize(ip_addrs, node_id);
#endif

    std::vector<uint32_t> members(num_nodes);
    for(uint i = 0; i < num_nodes; ++i) {
        members[i] = i;
    }

    uint32_t num_senders = num_nodes, row_offset = 0;
    if(num_senders_selector == 0) {
    } else if(num_senders_selector == 1) {
        num_senders = num_nodes / 2;
        row_offset = (num_nodes + 1) / 2;
    } else {
        num_senders = 1;
        row_offset = num_nodes - 1;
    }

    std::shared_ptr<multicast_sst> sst = make_shared<multicast_sst>(
            sst::SSTParams(members, node_id),
            window_size,
            num_senders);

    auto check_failures_loop = [&sst]() {
        pthread_setname_np(pthread_self(), "check_failures");
        while(true) {
            std::this_thread::sleep_for(chrono::microseconds(1000));
            if(sst) {
                sst->put_with_completion((char*)std::addressof(sst->heartbeat[0]) - sst->getBaseAddress(), sizeof(bool));
            }
        }
    };

    thread failures_thread = std::thread(check_failures_loop);

    vector<bool> completed(num_senders, false);
    uint num_finished = 0;
    auto sst_receive_handler = [&num_finished, num_senders_selector, &num_nodes, &num_messages, &completed](
            uint32_t sender_rank, uint64_t index,
            volatile char* msg, uint32_t size) {
        if(index == num_messages - 1) {
            completed[sender_rank] = true;
            num_finished++;
        }
        if(num_finished == num_nodes || (num_senders_selector == 1 && num_finished == num_nodes / 2) || (num_senders_selector == 2 && num_finished == 1)) {
            done = true;
        }
    };
    auto receiver_pred = [window_size, num_nodes, node_id](const multicast_sst& sst) {
        return true;
    };
    vector<int64_t> last_max_num_received(num_senders, -1);
    auto receiver_trig = [&completed, last_max_num_received, window_size, num_nodes, node_id, sst_receive_handler,
                          row_offset, num_senders](multicast_sst& sst) mutable {
        while(true) {
            for(uint j = 0; j < num_senders; ++j) {
                auto num_received = sst.num_received_sst[node_id][j] + 1;
                uint32_t slot = num_received % window_size;
                if((int64_t)sst.slots[row_offset + j][slot].next_seq == (num_received / window_size + 1)) {
                    sst_receive_handler(j, num_received,
                                        sst.slots[row_offset + j][slot].buf,
                                        sst.slots[row_offset + j][slot].size);
                    sst.num_received_sst[node_id][j]++;
                }
            }
            bool time_to_push = true;
            for(uint j = 0; j < num_senders; ++j) {
                if(completed[j]) {
                    continue;
                }
                if(sst.num_received_sst[node_id][j] - last_max_num_received[j] <= window_size / 2) {
                    time_to_push = false;
                }
            }
            if(time_to_push) {
                break;
            }
        }
        sst.put(sst.num_received_sst.get_base() - sst.getBaseAddress(),
                sizeof(sst.num_received_sst[0][0]) * num_senders);
        for(uint j = 0; j < num_senders; ++j) {
            last_max_num_received[j] = sst.num_received_sst[node_id][j];
        }
    };
    // inserting later

    vector<uint32_t> indices(num_nodes);
    iota(indices.begin(), indices.end(), 0);
    std::vector<int> is_sender(num_nodes, 1);
    if(num_senders_selector == 0) {
    } else if(num_senders_selector == 1) {
        for(uint i = 0; i <= (num_nodes - 1) / 2; ++i) {
            is_sender[i] = 0;
        }
    } else {
        for(uint i = 0; i < num_nodes - 1; ++i) {
            is_sender[i] = 0;
        }
    }
    sst::multicast_group<multicast_sst> g(sst, indices, window_size, is_sender);
    // now
    sst->predicates.insert(receiver_pred, receiver_trig,
                           sst::PredicateType::RECURRENT);
    // uint count = 0;
    struct timespec start_time, end_time;
    // start timer
    clock_gettime(CLOCK_REALTIME, &start_time);
    if(node_id == num_nodes - 1 || num_senders_selector == 0 || (node_id > (num_nodes - 1) / 2 && num_senders_selector == 1)) {
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

    double sum_message_rate = aggregate_bandwidth(members, node_id, message_rate);
    log_results(exp_results{num_nodes, num_senders_selector, max_msg_size, sum_message_rate},
                "data_multicast");
    sst->sync_with_members();
}
