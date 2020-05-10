#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <time.h>
#include <vector>

#include "aggregate_bandwidth.hpp"
#include "log_results.hpp"
#include <derecho/core/derecho.hpp>

using std::cout;
using std::endl;
using std::map;
using std::vector;

using namespace derecho;

struct exp_result {
    uint32_t num_nodes;
    uint num_slow_senders_selector;
    long long unsigned int max_msg_size;
    unsigned int window_size;
    uint num_messages;
    uint wait_time;
    double bw;

    void print(std::ofstream& fout) {
        fout << num_nodes << " "
             << max_msg_size << " " << window_size << " "
             << num_messages << " " << wait_time << " "
             << num_slow_senders_selector << " "
             << bw << endl;
    }
};

void busy_wait(uint32_t wait_time) {
    struct timespec start_time, end_time;
    clock_gettime(CLOCK_REALTIME, &start_time);

    if(wait_time == 0) {
        // wait forever
        while(true) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

    while(true) {
        clock_gettime(CLOCK_REALTIME, &end_time);
        long long int nanoseconds_elapsed = (end_time.tv_sec - start_time.tv_sec) * (long long int)1e9 + (end_time.tv_nsec - start_time.tv_nsec);
        if(nanoseconds_elapsed >= wait_time * 1000) {
            return;
        }
    }
}

int main(int argc, char* argv[]) {
    if(argc < 5 || (argc > 5 && strcmp("--", argv[argc - 5]))) {
        cout << "Invalid command line arguments." << endl;
        cout << "USAGE:" << argv[0] << "[ derecho-config-list -- ] num_nodes, num_slow_senders_selector (0 - 0 senders, 1 - half senders, 2 - one sender), wait_time (in us), num_messages" << endl;
        cout << "Thank you" << endl;
        return -1;
    }
    pthread_setname_np(pthread_self(), "slow_senders");

    // initialize the special arguments for this test
    const uint num_nodes = std::stoi(argv[argc - 4]);
    const uint num_slow_senders_selector = std::stoi(argv[argc - 3]);
    const uint wait_time = std::stoi(argv[argc - 2]);
    const uint num_messages = std::stoi(argv[argc - 1]);

    // Read configurations from the command line options as well as the default config file
    Conf::initialize(argc, argv);

    // variable 'done' tracks the end of the test
    volatile bool done = false;
    // start and end time to compute bw
    struct timespec start_time;
    long long int nanoseconds_elapsed;
    // how many slow senders
    uint num_slow_senders = 0;
    if(num_slow_senders_selector == 1) {
        num_slow_senders = num_nodes / 2;
    } else if (num_slow_senders_selector == 2) {
        num_slow_senders = 1;
    }

    // callback into the application code at each message delivery
    auto stability_callback = [&num_messages,
                               &done,
                               &num_nodes,
                               &start_time,
                               &nanoseconds_elapsed,
                               num_slow_senders,
                               num_delivered = 0u](uint32_t subgroup, uint32_t sender_id, long long int index, std::optional<std::pair<char*, long long int>> data, persistent::version_t ver) mutable {
        // increment the total number of messages delivered
        ++num_delivered;
        if(num_delivered == num_messages * (num_nodes-num_slow_senders)) {
            // here take the time
            struct timespec end_time;
            clock_gettime(CLOCK_REALTIME, &end_time);
            // compute time elapsed btw the start and the delivery of a specific message (not the last one)
            nanoseconds_elapsed = (end_time.tv_sec - start_time.tv_sec) * (long long int)1e9 + (end_time.tv_nsec - start_time.tv_nsec);
        }
        if(num_delivered == num_messages * num_nodes) {
            done = true;
        }
    };

    auto membership_function = [num_nodes](
                                       const std::vector<std::type_index>& subgroup_type_order,
                                       const std::unique_ptr<View>& prev_view, View& curr_view) {
        subgroup_shard_layout_t subgroup_vector(1);
        auto num_members = curr_view.members.size();
        // wait for all nodes to join the group
        if(num_members < num_nodes) {
            throw subgroup_provisioning_exception();
        }
        subgroup_vector[0].emplace_back(curr_view.make_subview(curr_view.members));
        curr_view.next_unassigned_rank = curr_view.members.size();
        //Since we know there is only one subgroup type, just put a single entry in the map
        derecho::subgroup_allocation_map_t subgroup_allocation;
        subgroup_allocation.emplace(std::type_index(typeid(RawObject)), std::move(subgroup_vector));
        return subgroup_allocation;
    };

    //Wrap the membership function in a SubgroupInfo
    SubgroupInfo one_raw_group(membership_function);

    // join the group
    Group<RawObject> group(CallbackSet{stability_callback},
                           one_raw_group, nullptr, std::vector<view_upcall_t>{},
                           &raw_object_factory);

    cout << "Finished constructing/joining Group" << endl;
    auto members_order = group.get_members();
    uint32_t node_rank = group.get_my_rank();

    long long unsigned int max_msg_size = getConfUInt64(CONF_SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE);

    // this function sends all the messages
    auto send_fast = [&]() {
        Replicated<RawObject>& raw_subgroup = group.get_subgroup<RawObject>();
        for(uint i = 0; i < num_messages; ++i) {
            // the lambda function writes the message contents into the provided memory buffer
            // in this case, we do not touch the memory region
            raw_subgroup.send(max_msg_size, [](char* buf) {});
        }
    };

    auto send_slow = [&]() {
        Replicated<RawObject>& raw_subgroup = group.get_subgroup<RawObject>();
        for(uint i = 0; i < num_messages; ++i) {
            busy_wait(wait_time);
            // the lambda function writes the message contents into the provided memory buffer
            // in this case, we do not touch the memory region
            raw_subgroup.send(max_msg_size, [](char* buf) {});
        }
    };

    // start timer
    clock_gettime(CLOCK_REALTIME, &start_time);
    // send all messages or skip if not a sender
    if(num_slow_senders_selector == 0) {
        send_fast();
    } else if(num_slow_senders_selector == 1) {
        if(node_rank > (num_nodes - 1) / 2) {
            send_slow();
        } else {
            send_fast();
        }
    } else {
        if(node_rank == num_nodes - 1) {
            send_slow();
        } else {
            send_fast();
        }
    }
    // wait for the test to finish
    while(!done) {
    }
    // calculate bandwidth measured locally
    double bw = (max_msg_size * num_messages * (num_nodes-num_slow_senders) + 0.0) / nanoseconds_elapsed;
    // aggregate bandwidth from all nodes
    double avg_bw = aggregate_bandwidth(members_order, members_order[node_rank], bw);
    // log the result at the leader node
    if(node_rank == 0) {
        log_results(exp_result{num_nodes, num_slow_senders_selector, max_msg_size,
                               getConfUInt32(CONF_SUBGROUP_DEFAULT_WINDOW_SIZE), num_messages,
                               wait_time, avg_bw},
                    "data_sender_delay_test");
    }

    group.barrier_sync();
    group.leave();
}