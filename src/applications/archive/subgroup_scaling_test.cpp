/* Same experiment as derecho_bw_test.cpp really,
 * but creates varying number of subgroups and sends messages in all
 * Better to separate it so that it can be managed independently
 */
#include <atomic>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <time.h>
#include <typeindex>
#include <vector>

#include "aggregate_bandwidth.hpp"
#include <derecho/core/derecho.hpp>
#include "log_results.hpp"
#include <derecho/rdmc/rdmc.hpp>

using std::cout;
using std::endl;
using std::map;
using std::vector;

using namespace derecho;

template <typename T>
struct volatile_wrapper {
    volatile T t;
    volatile_wrapper(T t) : t(t) {}
    bool operator<(const T t1) {
        return t < t1;
    }
};

struct exp_result {
    uint32_t num_nodes;
    long long unsigned int max_msg_size;
    uint32_t num_subgroups;
    uint32_t subgroup_size;
    double bw;

    void print(std::ofstream &fout) {
        fout << num_nodes << " "
             << max_msg_size << " "
             << num_subgroups << " "
             << subgroup_size << " "
             << bw << endl;
    }
};

int main(int argc, char *argv[]) {
    try {
        if(argc != 6) {
            cout << "Invalid command line arguments." << endl;
            cout << "USAGE:" << argv[0] << " num_nodes, num_groups, subgroup_size, num_messages, msg_size" << endl;
            cout << "Thank you" << endl;
            return -1;
        }
        pthread_setname_np(pthread_self(), "sbgrp_scaling");

        const uint32_t num_nodes = std::stoi(argv[1]);
        const uint32_t num_subgroups = std::stoi(argv[2]);
        const uint32_t subgroup_size = std::stoi(argv[3]);
        const uint32_t num_messages = std::stoi(argv[4]);
        const uint64_t max_msg_size = std::stoi(argv[5]);

        if(subgroup_size > num_nodes) {
            cout << "Subgroup size must be at most num nodes!" << endl;
            cout << "USAGE:" << argv[0] << " num_nodes, subgroup_size, num_messages, msg_size" << endl;
            cout << "Thank you" << endl;
            return -1;
        }

        // Read configurations from the command line options as well as the default config file
        Conf::initialize(argc, argv);
        const uint32_t node_id = getConfUInt32(CONF_DERECHO_LOCAL_ID);

       /* Create subgroups.
        * Subgroup creation is based on the node index.
        */
        vector<uint32_t> send_subgroup_indices;
        for(uint i = 0; i < subgroup_size; ++i) {
            auto j = ((int32_t)(node_id + num_nodes) - (int32_t)i) % num_nodes;
	    if(j < num_subgroups) {
                send_subgroup_indices.push_back(j);
            }
        }

	volatile bool done = false;
	volatile uint64_t num_delivered = 0;

        auto stability_callback = [&](uint32_t subgroup_num, int sender_id, long long int index,
                                                              std::optional<std::pair<char*, long long int>> data, persistent::version_t ver) mutable {
	    ++num_delivered;
//	    std::cout << "Received " << subgroup_num << "." << index << std::endl;
	    if(num_delivered == num_messages * subgroup_size * send_subgroup_indices.size()) {
                done = true;
            }
        };

        auto membership_function = [num_subgroups, num_nodes, subgroup_size, node_id](
                const std::vector<std::type_index>& subgroup_type_order,
                const std::unique_ptr<derecho::View>& prev_view, derecho::View& curr_view) {
            derecho::subgroup_allocation_map_t subgroup_allocation;
            auto num_members = curr_view.members.size();
            if(num_members < num_nodes) {
                throw subgroup_provisioning_exception();
            }
            subgroup_shard_layout_t subgroup_vector(num_subgroups);
            for(uint i = 0; i < num_subgroups; ++i) {
                vector<uint32_t> members(subgroup_size);
                for(uint j = 0; j < subgroup_size; ++j) {
                    members[j] = (node_id + j) % num_members;
                }
                subgroup_vector[i].emplace_back(curr_view.make_subview(members));
            }
            curr_view.next_unassigned_rank = curr_view.members.size();
            subgroup_allocation.emplace(std::type_index(typeid(RawObject)), std::move(subgroup_vector));
            return subgroup_allocation;
	};

        SubgroupInfo raw_groups(membership_function);

        Group<RawObject> managed_group(CallbackSet{stability_callback},
                              raw_groups, nullptr, std::vector<view_upcall_t>{},
                              &raw_object_factory);

        cout << "Finished constructing/joining ManagedGroup" << endl;

        while(managed_group.get_members().size() < num_nodes) {
        }
        auto members_order = managed_group.get_members();
        auto node_rank = managed_group.get_my_rank();

        vector<std::reference_wrapper<Replicated<RawObject>>> subgroups;
        for(uint i = 0; i < subgroup_size; ++i) {
            subgroups.emplace_back(managed_group.get_subgroup<RawObject>((node_id - i + num_subgroups) % num_subgroups));
        }

        auto send_some = [&](uint32_t num_subgroups) {
            const auto num_subgroups_to_send = send_subgroup_indices.size();
            for(uint i = 0; i < num_subgroups * num_messages; ++i) {
		uint j = i % num_subgroups_to_send;
		subgroups[j].get().send(max_msg_size, [](char* buf){});
            }
        };

        struct timespec start_time, end_time;
        long long int nanoseconds_elapsed;
        double bw, avg_bw;
        // start timer
        clock_gettime(CLOCK_REALTIME, &start_time);
        send_some(num_subgroups);
        while(!done) {
        }
        clock_gettime(CLOCK_REALTIME, &end_time);
        nanoseconds_elapsed = (end_time.tv_sec - start_time.tv_sec) * (long long int)1e9 + (end_time.tv_nsec - start_time.tv_nsec);
        bw = (max_msg_size * num_messages * num_nodes * send_subgroup_indices.size() + 0.0) / nanoseconds_elapsed;
        avg_bw = aggregate_bandwidth(members_order, node_id, bw);
        if(node_rank == 0) {
            log_results(exp_result{num_nodes, max_msg_size, num_subgroups, subgroup_size, avg_bw},
                        "data_subgroup_scaling");
        }

        managed_group.barrier_sync();
        // managed_group.leave();
        // sst::verbs_destroy();
        exit(0);
        cout << "Finished destroying managed_group" << endl;
        std::this_thread::sleep_for(std::chrono::seconds(10));
    } catch(const std::exception &e) {
        cout << "Exception in main: " << e.what() << endl;
        cout << "main shutting down" << endl;
    }
}
