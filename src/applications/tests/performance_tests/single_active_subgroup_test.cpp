#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <time.h>
#include <vector>

#include "aggregate_bandwidth.hpp"
#include <derecho/core/derecho.hpp>
#include "log_results.hpp"

using std::cout;
using std::endl;
using std::map;
using std::vector;

using namespace derecho;

struct exp_result {
    uint32_t num_nodes;
    long long unsigned int max_msg_size;
    uint32_t window_size;
    uint32_t num_messages;
    uint32_t num_subgroups;
    double bw;

    void print(std::ofstream& fout) {
        fout << num_nodes << " "
             << max_msg_size << " " << window_size << " "
             << num_messages << " "
	     << num_subgroups << " "
             << bw << endl;
    }
};

int main(int argc, char* argv[]) {
    if(argc < 4 || (argc > 4 && strcmp("--", argv[argc - 4]))) {
        cout << "Invalid command line arguments." << endl;
        cout << "Usage:" << argv[0]
             << "[ derecho-config-list -- ] num_nodes, num_subgroups, num_messages"
             << endl;
        return 1;
    }
    pthread_setname_np(pthread_self(), "main");

    // initialize the special arguments for this test
    const uint num_nodes = std::stoi(argv[argc - 3]);
    const uint num_subgroups = std::stoi(argv[argc - 2]);
    const uint num_messages = std::stoi(argv[argc - 1]);

    // Read configurations from the command line options as well as the default config file
    Conf::initialize(argc, argv);

    // variable 'done' tracks the end of the test
    volatile bool done = false;
    // callback into the application code at each message delivery
    auto stability_callback = [&num_messages,
                               &done,
                               &num_nodes,
                               num_delivered = 0u](uint32_t subgroup, uint32_t sender_id, long long int index, std::optional<std::pair<char*, long long int>> data, persistent::version_t ver) mutable {
        // increment the total number of messages delivered
        ++num_delivered;
        if(num_delivered == num_messages * num_nodes) {
            done = true;
        }
    };

    auto membership_function = [num_subgroups, num_nodes](
            const std::vector<std::type_index>& subgroup_type_order,
            const std::unique_ptr<View>& prev_view, View& curr_view) {
        subgroup_shard_layout_t subgroup_vector(num_subgroups);
        auto num_members = curr_view.members.size();
        // wait for all nodes to join the group
        if(num_members < num_nodes) {
            throw subgroup_provisioning_exception();
        }
        for(uint i = 0; i < num_subgroups; ++i) {
            subgroup_vector[i].emplace_back(curr_view.make_subview(curr_view.members));
        }
        curr_view.next_unassigned_rank = curr_view.members.size();
        //Since we know there is only one subgroup type, just put a single entry in the map
        derecho::subgroup_allocation_map_t subgroup_allocation;
        subgroup_allocation.emplace(std::type_index(typeid(RawObject)), std::move(subgroup_vector));
        return subgroup_allocation;
    };

    //Wrap the membership function in a SubgroupInfo
    SubgroupInfo raw_groups(membership_function);

    // join the group
    Group<RawObject> group(CallbackSet{stability_callback},
                           raw_groups, nullptr, std::vector<view_upcall_t>{},
                           &raw_object_factory);

    cout << "Finished constructing/joining Group" << endl;
    auto members_order = group.get_members();
    uint32_t node_rank = group.get_my_rank();

    long long unsigned int max_msg_size = getConfUInt64(CONF_SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE);

    // this function sends all the messages
    auto send_in_one_subgroup = [&]() {
        Replicated<RawObject>& raw_subgroup = group.get_subgroup<RawObject>(0);
        for(uint i = 0; i < num_messages; ++i) {
            // the lambda function writes the message contents into the provided memory buffer
            // in this case, we do not touch the memory region
            raw_subgroup.send(max_msg_size, [](char* buf) {});
        }
    };

    // This is a temporary patch
    group.barrier_sync();
    std::this_thread::sleep_for(std::chrono::milliseconds(5000));
    group.barrier_sync();

    struct timespec start_time;
    // start timer
    clock_gettime(CLOCK_REALTIME, &start_time);
    send_in_one_subgroup();
    // wait for the test to finish
    while(!done) {
    }
    // end timer
    struct timespec end_time;
    clock_gettime(CLOCK_REALTIME, &end_time);
    long long int nanoseconds_elapsed = (end_time.tv_sec - start_time.tv_sec) * (long long int)1e9 + (end_time.tv_nsec - start_time.tv_nsec);
    // calculate bandwidth measured locally
    double bw;
    bw = (max_msg_size * num_messages * num_nodes + 0.0) / nanoseconds_elapsed;
    // aggregate bandwidth from all nodes
    double avg_bw = aggregate_bandwidth(members_order, members_order[node_rank], bw);
    // log the result at the leader node
    if(node_rank == 0) {
        log_results(exp_result{num_nodes, max_msg_size,
                               getConfUInt32(CONF_SUBGROUP_DEFAULT_WINDOW_SIZE),
                               num_messages,
			       num_subgroups,
                               avg_bw},
                    "data_single_active_subgroup");
    }

    group.barrier_sync();
    group.leave();
}