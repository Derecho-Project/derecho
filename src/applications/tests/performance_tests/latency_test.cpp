/*
 * This test measures the latency of Derecho raw (uncooked) sends in microseconds as a function of
 * 1. the number of nodes 
 * 2. the number of senders (all sending, half nodes sending, one sending)
 * 3. number of messages sent per sender
 * 4. delivery mode (atomic multicast or unordered)
 * Other parameters are retrieved directly from the derecho.cfg file or through the derecho-config-list
 * set of parameters.
 * The test waits for every node to join and then each sender starts sending messages continuously
 * in the only subgroup that consists of all the nodes
 * Upon completion, the results are appended to file data_latency on the leader
 */
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include <derecho/core/derecho.hpp>
#include <derecho/utils/time.h>

#include "aggregate_latency.hpp"
#include "log_results.hpp"

using std::cout;
using std::endl;
using std::vector;
using namespace derecho;

std::unique_ptr<rdmc::barrier_group> universal_barrier_group;

struct exp_result {
    uint32_t num_nodes;
    long long unsigned int max_msg_size;
    uint32_t num_senders_selector;
    uint32_t delivery_mode;
    double latency;
    double stddev;

    void print(std::ofstream& fout) {
        fout << num_nodes << " " << max_msg_size << " "
             << num_senders_selector << " "
             << delivery_mode << " " << latency << " "
             << stddev << endl;
    }
};

int main(int argc, char* argv[]) {
    if(argc < 5 || (argc > 5 && strcmp("--", argv[argc - 5]))) {
        cout << "Insufficient number of command line arguments" << endl;
        cout << "USAGE:" << argv[0] << "[ derecho-config-list -- ] num_nodes, num_senders_selector (0 - all senders, 1 - half senders, 2 - one sender), num_messages, delivery_mode (0 - ordered mode, 1 - unordered mode)" << endl;
        return -1;
    }
    pthread_setname_np(pthread_self(), "latency_test");

    // initialize the special arguments for this test
    uint32_t num_nodes = std::stoi(argv[argc - 4]);
    const uint32_t num_senders_selector = std::stoi(argv[argc - 3]);
    const uint32_t num_messages = std::stoi(argv[argc - 2]);
    const uint32_t delivery_mode = std::stoi(argv[argc - 1]);

    // Read configurations from the command line options as well as the default config file
    Conf::initialize(argc, argv);
    const uint64_t msg_size = getConfUInt64(CONF_SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE);

    // used by the sending nodes to track time of delivery of messages
    vector<struct timespec> start_times(num_messages), end_times(num_messages);

    // variable 'done' tracks the end of the test
    volatile bool done = false;
    uint32_t my_id;
    // callback into the application code at each message delivery
    auto stability_callback = [&, num_delivered = 0u, time_index = 0u](
                                      int32_t subgroup, uint32_t sender_id, long long int index,
                                      std::optional<std::pair<char*, long long int>> data,
                                      persistent::version_t ver) mutable {
        // increment the total number of messages delivered
        ++num_delivered;
        if(sender_id == my_id) {
            // if I am the sender for this message, measure the time of delivery
            clock_gettime(CLOCK_REALTIME, &end_times[time_index++]);
        }
        if(num_senders_selector == 0) {
            if(num_delivered == num_messages * num_nodes) {
                done = true;
            }
        } else if(num_senders_selector == 1) {
            if(num_delivered == num_messages * (num_nodes / 2)) {
                done = true;
            }
        } else {
            if(num_delivered == num_messages) {
                done = true;
            }
        }
    };

    Mode mode = Mode::ORDERED;
    if(delivery_mode) {
        mode = Mode::UNORDERED;
    }

    auto membership_function = [num_senders_selector, mode, num_nodes](
                                       const std::vector<std::type_index>& subgroup_type_order,
                                       const std::unique_ptr<View>& prev_view, View& curr_view) {
        subgroup_shard_layout_t subgroup_vector(1);
        auto num_members = curr_view.members.size();
        // wait for all nodes to join the group
        if(num_members < num_nodes) {
            throw subgroup_provisioning_exception();
        }
        // all senders case
        if(num_senders_selector == 0) {
            // a call to make_subview without the sender information
            // defaults to all members sending
            subgroup_vector[0].emplace_back(curr_view.make_subview(curr_view.members, mode));
        } else {
            // configure the number of senders
            vector<int> is_sender(num_members, 1);
            // half senders case
            if(num_senders_selector == 1) {
                // mark members ranked 0 to num_members/2 as non-senders
                for(uint i = 0; i <= (num_members - 1) / 2; ++i) {
                    is_sender[i] = 0;
                }
            } else {
                // mark all members except the last ranked one as non-senders
                for(uint i = 0; i < num_members - 1; ++i) {
                    is_sender[i] = 0;
                }
            }
            // provide the sender information in a call to make_subview
            subgroup_vector[0].emplace_back(curr_view.make_subview(curr_view.members, mode, is_sender));
        }
        curr_view.next_unassigned_rank = curr_view.members.size();
        //Since we know there is only one subgroup type, just put a single entry in the map
        derecho::subgroup_allocation_map_t subgroup_allocation;
        subgroup_allocation.emplace(std::type_index(typeid(RawObject)), std::move(subgroup_vector));
        return subgroup_allocation;
    };

    //Wrap the membership function in a SubgroupInfo
    SubgroupInfo one_raw_group(membership_function);

    Group<RawObject> managed_group(CallbackSet{stability_callback}, one_raw_group, nullptr,
                                   std::vector<view_upcall_t>{},
                                   &raw_object_factory);
    cout << "All nodes joined." << endl;

    auto group_members = managed_group.get_members();
    uint32_t my_rank = managed_group.get_my_rank();
    my_id = group_members[my_rank];

    Replicated<RawObject>& group_as_subgroup = managed_group.get_subgroup<RawObject>();
    auto send_all = [&]() {
        for(uint i = 0; i < num_messages; ++i) {
            // the lambda function writes the message contents into the provided memory buffer
            // in this case, we do not touch the memory region
            group_as_subgroup.send(msg_size, [&](char* buf) {
                clock_gettime(CLOCK_REALTIME, &start_times[i]);
            });
        }
    };
    // send all messages or skip if not a sender
    if(num_senders_selector == 0) {
        send_all();
    } else if(num_senders_selector == 1) {
        if(my_rank > (num_nodes - 1) / 2) {
            send_all();
        }
    } else {
        if(my_rank == num_nodes - 1) {
            send_all();
        }
    }

    // wait for the test to finish
    while(!done) {
    }

    double avg_latency, avg_std_dev;
    // the if loop selects the senders
    if(num_senders_selector == 0 || (num_senders_selector == 1 && my_rank > (num_nodes - 1) / 2) || (num_senders_selector == 2 && my_rank == num_nodes - 1)) {
        double total_time = 0;
        double sum_of_square = 0.0;
        double average_time = 0.0;
        for(uint i = 0; i < num_messages; ++i) {
            total_time += (end_times[i].tv_sec - start_times[i].tv_sec) * (long long int)1e9 + (end_times[i].tv_nsec - start_times[i].tv_nsec);
        }
        // average latency in nano seconds
        average_time = (total_time / num_messages);
        // calculate the standard deviation
        for(uint i = 0; i < num_messages; ++i) {
            sum_of_square += (double)((end_times[i].tv_sec - start_times[i].tv_sec) * (long long int)1e9 + (end_times[i].tv_nsec - start_times[i].tv_nsec) - average_time) * ((end_times[i].tv_sec - start_times[i].tv_sec) * (long long int)1e9 + (end_times[i].tv_nsec - start_times[i].tv_nsec) - average_time);
        }
        double std_dev = sqrt(sum_of_square / (num_messages - 1));
        // aggregate latency values from all senders
        std::tie(avg_latency, avg_std_dev) = aggregate_latency(group_members, my_id, (average_time / 1000.0), (std_dev / 1000.0));
    } else {
        // if not a sender, then pass 0 as the latency (not counted)
        std::tie(avg_latency, avg_std_dev) = aggregate_latency(group_members, my_id, 0.0, 0.0);
    }

    // log the result at the leader node
    if(my_rank == 0) {
        log_results(exp_result{num_nodes, msg_size, num_senders_selector, delivery_mode, avg_latency, avg_std_dev}, "data_latency");
    }
    managed_group.barrier_sync();
    managed_group.leave();
}