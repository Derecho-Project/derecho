#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <time.h>
#include <vector>

#ifndef NDEBUG
#include <spdlog/spdlog.h>
#endif

#include "aggregate_bandwidth.h"
#include "derecho/derecho.h"
#include "log_results.h"
#include "rdmc/rdmc.h"
#include "rdmc/util.h"

using std::cout;
using std::endl;
using std::map;
using std::vector;

using namespace derecho;

struct exp_result {
    uint32_t num_nodes;
    uint num_senders_selector;
    long long unsigned int max_msg_size;
    unsigned int window_size;
    uint num_messages;
    uint delivery_mode;
    double bw;

    void print(std::ofstream &fout) {
        fout << num_nodes << " " << num_senders_selector << " "
             << max_msg_size << " " << window_size << " "
             << num_messages << " " << delivery_mode << " "
             << bw << endl;
    }
};

int main(int argc, char *argv[]) {
    if(argc < 5) {
        cout << "Insufficient number of command line arguments" << endl;
        cout << "Enter num_nodes, num_senders_selector (0 - all senders, 1 - half senders, 2 - one sender), num_messages, delivery_mode (0 - ordered mode, 1 - unordered mode)" << endl;
        cout << "Thank you" << endl;
        return -1;
    }
    pthread_setname_np(pthread_self(), "bw_test");
    srand(time(NULL));

    const uint num_nodes = std::stoi(argv[1]);
    const uint num_senders_selector = std::stoi(argv[2]);
    const uint num_messages = std::stoi(argv[3]);
    const uint delivery_mode = std::stoi(argv[4]);

    Conf::initialize(argc, argv);

    volatile bool done = false;
    auto stability_callback = [&num_messages,
                               &done,
                               &num_nodes,
                               num_senders_selector,
                               num_total_received = 0u](uint32_t subgroup, int sender_id, long long int index, char *buf, long long int msg_size) mutable {
        // null message filter
        if(msg_size == 0) {
            return;
        }

        ++num_total_received;
        if(num_senders_selector == 0) {
            if(num_total_received == num_messages * num_nodes) {
                done = true;
            }
        } else if(num_senders_selector == 1) {
            if(num_total_received == num_messages * (num_nodes / 2)) {
                done = true;
            }
        } else {
            if(num_total_received == num_messages) {
                done = true;
            }
        }
    };

    Mode mode = Mode::ORDERED;
    if(delivery_mode) {
        mode = Mode::UNORDERED;
    }

    auto membership_function = [num_senders_selector, mode, num_nodes](const View &curr_view, int &next_unassigned_rank) {
        subgroup_shard_layout_t subgroup_vector(1);
        auto num_members = curr_view.members.size();
        if(num_members < num_nodes) {
            throw subgroup_provisioning_exception();
        }
        if(num_senders_selector == 0) {
            subgroup_vector[0].emplace_back(curr_view.make_subview(curr_view.members, mode));
        } else {
            std::vector<int> is_sender(num_members, 1);
            if(num_senders_selector == 1) {
                for(uint i = 0; i <= (num_members - 1) / 2; ++i) {
                    is_sender[i] = 0;
                }
            } else {
                for(uint i = 0; i < num_members - 1; ++i) {
                    is_sender[i] = 0;
                }
            }
            subgroup_vector[0].emplace_back(curr_view.make_subview(curr_view.members, mode, is_sender));
        }
        next_unassigned_rank = curr_view.members.size();
        return subgroup_vector;
    };

    std::map<std::type_index, shard_view_generator_t> subgroup_map = {{std::type_index(typeid(RawObject)), membership_function}};
    SubgroupInfo one_raw_group(subgroup_map);

    Group<> managed_group(CallbackSet{stability_callback},
                                   one_raw_group);

    cout << "Finished constructing/joining ManagedGroup" << endl;

    while(managed_group.get_members().size() < num_nodes) {
    }
    uint32_t node_id = getConfUInt32(CONF_DERECHO_LOCAL_ID);
    uint32_t node_rank = -1;
    auto members_order = managed_group.get_members();
    cout << "The order of members is :" << endl;
    for(uint i = 0; i < num_nodes; ++i) {
        cout << members_order[i] << " ";
        if(members_order[i] == node_id) {
            node_rank = i;
        }
    }
    cout << endl;

    long long unsigned int max_msg_size = getConfUInt64(CONF_DERECHO_MAX_PAYLOAD_SIZE);
    
    auto send_all = [&]() {
        RawSubgroup &group_as_subgroup = managed_group.get_subgroup<RawObject>();
        for(uint i = 0; i < num_messages; ++i) {
            // cout << "Asking for a buffer" << endl;
	  char *buf = group_as_subgroup.get_sendbuffer_ptr(max_msg_size);
            while(!buf) {
                buf = group_as_subgroup.get_sendbuffer_ptr(max_msg_size);
            }
            // cout << "Obtained a buffer, sending" << endl;
            group_as_subgroup.send();
        }
    };

    struct timespec start_time;
    // start timer
    clock_gettime(CLOCK_REALTIME, &start_time);
    if(num_senders_selector == 0) {
        send_all();
    } else if(num_senders_selector == 1) {
        if(node_rank > (num_nodes - 1) / 2) {
            send_all();
        }
    } else {
        if(node_rank == num_nodes - 1) {
            send_all();
        }
    }
    while(!done) {
    }
    struct timespec end_time;
    clock_gettime(CLOCK_REALTIME, &end_time);
    long long int nanoseconds_elapsed = (end_time.tv_sec - start_time.tv_sec) * (long long int)1e9 + (end_time.tv_nsec - start_time.tv_nsec);
    double bw;
    if(num_senders_selector == 0) {
        bw = (max_msg_size * num_messages * num_nodes + 0.0) / nanoseconds_elapsed;
    } else if(num_senders_selector == 1) {
        bw = (max_msg_size * num_messages * (num_nodes / 2) + 0.0) / nanoseconds_elapsed;
    } else {
        bw = (max_msg_size * num_messages + 0.0) / nanoseconds_elapsed;
    }
    double avg_bw = aggregate_bandwidth(members_order, node_id, bw);
    if(node_rank == 0) {
        log_results(exp_result{num_nodes, num_senders_selector, max_msg_size,
                               getConfUInt32(CONF_DERECHO_WINDOW_SIZE), num_messages,
                               delivery_mode, avg_bw},
                    "data_derecho_bw");
    }

    managed_group.barrier_sync();
    managed_group.leave();
    // sst::lf_destroy();
}
