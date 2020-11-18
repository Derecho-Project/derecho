/*
 * This test measures the bandwidth of Derecho raw (uncooked) sends in GB/s as a function of
 * 1. the number of nodes 2. the number of senders (all sending, half nodes sending, one sending)
 * 3. message size 4. window size 5. number of messages sent per sender
 * 6. delivery mode (atomic multicast or unordered)
 * The test waits for every node to join and then each sender starts sending messages continuously
 * in the only subgroup that consists of all the nodes
 * Upon completion, the results are appended to file data_derecho_bw on the leader
 */
#include <chrono>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <vector>

#include <derecho/core/derecho.hpp>

#include "aggregate_bandwidth.hpp"
#include "log_results.hpp"
#include "partial_senders_allocator.hpp"

using std::cout;
using std::endl;
using std::map;
using std::vector;

using namespace derecho;

struct exp_result {
    uint32_t num_nodes;
    uint32_t num_senders_selector;
    long long unsigned int max_msg_size;
    unsigned int window_size;
    uint32_t num_messages;
    uint32_t delivery_mode;
    double bw;

    void print(std::ofstream& fout) {
        fout << num_nodes << " " << num_senders_selector << " "
             << max_msg_size << " " << window_size << " "
             << num_messages << " " << delivery_mode << " "
             << bw << endl;
    }
};

#define DEFAULT_PROC_NAME "bw_test"

int main(int argc, char* argv[]) {
    int dashdash_pos = argc - 1;
    while(dashdash_pos > 0) {
        if(strcmp(argv[dashdash_pos], "--") == 0) {
            break;
        }
        dashdash_pos--;
    }

    if((argc - dashdash_pos) < 5) {
        cout << "Invalid command line arguments." << endl;
        cout << "USAGE: " << argv[0] << " [ derecho-config-list -- ] num_nodes, sender_selector (0 - all senders, 1 - half senders, 2 - one sender), num_messages, delivery_mode (0 - ordered mode, 1 - unordered mode) [proc_name]" << endl;
        std::cout << "Note: proc_name sets the process's name as displayed in ps and pkill commands, default is " DEFAULT_PROC_NAME << std::endl;
        return -1;
    }

    // initialize the special arguments for this test
    const uint32_t num_nodes = std::stoi(argv[dashdash_pos + 1]);
    const uint32_t num_senders_selector = std::stoi(argv[dashdash_pos + 2]);
    const uint32_t num_messages = std::stoi(argv[dashdash_pos + 3]);
    const uint32_t delivery_mode = std::stoi(argv[dashdash_pos + 4]);
    // Convert this integer to a more readable enum value
    const PartialSendMode senders_mode = num_senders_selector == 0
                                                 ? PartialSendMode::ALL_SENDERS
                                                 : (num_senders_selector == 1
                                                            ? PartialSendMode::HALF_SENDERS
                                                            : PartialSendMode::ONE_SENDER);

    if(dashdash_pos + 5 < argc) {
        pthread_setname_np(pthread_self(), argv[dashdash_pos + 5]);
    } else {
        pthread_setname_np(pthread_self(), DEFAULT_PROC_NAME);
    }
    // Read configurations from the command line options as well as the default config file
    Conf::initialize(argc, argv);

    // Compute the total number of messages that should be delivered
    uint64_t total_num_messages = 0;
    switch(senders_mode) {
        case PartialSendMode::ALL_SENDERS:
            total_num_messages = num_messages * num_nodes;
            break;
        case PartialSendMode::HALF_SENDERS:
            total_num_messages = num_messages * (num_nodes / 2);
            break;
        case PartialSendMode::ONE_SENDER:
            total_num_messages = num_messages;
            break;
    }

    // variable 'done' tracks the end of the test
    volatile bool done = false;
    // callback into the application code at each message delivery
    auto stability_callback = [&done,
                               total_num_messages,
                               num_delivered = 0u](uint32_t subgroup,
                                                   uint32_t sender_id,
                                                   long long int index,
                                                   std::optional<std::pair<char*, long long int>> data,
                                                   persistent::version_t ver) mutable {
        // Count the total number of messages delivered
        ++num_delivered;
        // Check for completion
        if(num_delivered == total_num_messages) {
            done = true;
        }
    };

    Mode mode = Mode::ORDERED;
    if(delivery_mode) {
        mode = Mode::UNORDERED;
    }

    auto membership_function = PartialSendersAllocator(num_nodes, senders_mode, mode);

    //Wrap the membership function in a SubgroupInfo
    SubgroupInfo one_raw_group(membership_function);

    // join the group
    Group<RawObject> group(CallbackSet{stability_callback},
                           one_raw_group, {}, std::vector<view_upcall_t>{},
                           &raw_object_factory);

    cout << "Finished constructing/joining Group" << endl;
    auto members_order = group.get_members();
    uint32_t node_rank = group.get_my_rank();

    long long unsigned int max_msg_size = getConfUInt64(CONF_SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE);

    // this function sends all the messages
    auto send_all = [&]() {
        Replicated<RawObject>& raw_subgroup = group.get_subgroup<RawObject>();
        for(uint i = 0; i < num_messages; ++i) {
            // the lambda function writes the message contents into the provided memory buffer
            // in this case, we do not touch the memory region
            raw_subgroup.send(max_msg_size, [](char* buf) {});
        }
    };

    // start timer
    auto start_time = std::chrono::steady_clock::now();
    // send all messages or skip if not a sender
    if(senders_mode == PartialSendMode::ALL_SENDERS) {
        send_all();
    } else if(senders_mode == PartialSendMode::HALF_SENDERS) {
        if(node_rank > (num_nodes - 1) / 2) {
            send_all();
        }
    } else {
        if(node_rank == num_nodes - 1) {
            send_all();
        }
    }
    // wait for the test to finish
    while(!done) {
    }
    // end timer
    auto end_time = std::chrono::steady_clock::now();
    long long int nanoseconds_elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    // calculate bandwidth measured locally
    double bw;
    if(senders_mode == PartialSendMode::ALL_SENDERS) {
        bw = (max_msg_size * num_messages * num_nodes + 0.0) / nanoseconds_elapsed;
    } else if(senders_mode == PartialSendMode::HALF_SENDERS) {
        bw = (max_msg_size * num_messages * (num_nodes / 2) + 0.0) / nanoseconds_elapsed;
    } else {
        bw = (max_msg_size * num_messages + 0.0) / nanoseconds_elapsed;
    }
    // aggregate bandwidth from all nodes
    double avg_bw = aggregate_bandwidth(members_order, members_order[node_rank], bw);
    // log the result at the leader node
    if(node_rank == 0) {
        log_results(exp_result{num_nodes, num_senders_selector, max_msg_size,
                               getConfUInt32(CONF_SUBGROUP_DEFAULT_WINDOW_SIZE), num_messages,
                               delivery_mode, avg_bw},
                    "data_derecho_bw");
    }

    group.barrier_sync();
    group.leave();
}
