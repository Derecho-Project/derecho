/* Same experiment as derecho_bw_test.cpp really, but creates varying number of subgroups and sends messages in all
* Better to separate it so that it can be managed independently */
#include <atomic>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <time.h>
#include <vector>

#include "aggregate_bandwidth.h"
#include "block_size.h"
#include "block_size.h"
#include "derecho/derecho.h"
#include "log_results.h"
#include "rdmc/rdmc.h"
#include "rdmc/util.h"

using std::vector;
using std::map;
using std::cout;
using std::endl;

using namespace derecho;

struct exp_result {
    uint32_t num_nodes;
    long long unsigned int max_msg_size;
    uint32_t subgroup_size;
    double bw;

    void print(std::ofstream &fout) {
        fout << num_nodes << " "
             << max_msg_size << " "
	     << subgroup_size << " "
             << bw << endl;
    }
};

int __attribute__((optimize("O0"))) main(int argc, char *argv[]) {
    try {
        if(argc < 3) {
            cout << "Insufficient number of command line arguments" << endl;
            cout << "Enter max_msg_size, subgroup_size" << endl;
            cout << "Thank you" << endl;
            exit(1);
        }
        pthread_setname_np(pthread_self(), "sbgrp_scaling");
        srand(time(NULL));

        uint32_t server_rank = 0;
        uint32_t node_id;
        uint32_t num_nodes;

        map<uint32_t, std::string> node_addresses;

        rdmc::query_addresses(node_addresses, node_id);
        num_nodes = node_addresses.size();

        vector<uint32_t> members(num_nodes);
        for(uint32_t i = 0; i < num_nodes; ++i) {
            members[i] = i;
        }

        const long long unsigned int max_msg_size = atoll(argv[1]);
        const long long unsigned int block_size = get_block_size(max_msg_size);
        const unsigned int window_size = ((max_msg_size < 20000) ? 15 : 3);
        const int send_medium = ((max_msg_size < 20000) ? 0 : 1);
        uint32_t num_messages = ((max_msg_size < 20000) ? 10000 : 1000);

        // will resize is as and when convenient
        uint32_t subgroup_size = atoi(argv[2]);
        map<uint32_t, uint32_t> subgroup_to_local_index;
        for(uint i = 0; i < subgroup_size; ++i) {
            subgroup_to_local_index[((int32_t)(node_id + num_nodes) - (int32_t)i) % num_nodes] = i;
        }
        vector<vector<long long int>> received_message_indices(subgroup_size);
        for(uint i = 0; i < subgroup_size; ++i) {
            received_message_indices[i].resize(subgroup_size, -1);
        }
        auto stability_callback = [&num_messages,
                                   &num_nodes,
                                   subgroup_to_local_index,
                                   &received_message_indices](uint32_t subgroup_num, int sender_id, long long int index,
                                                              char *buf, long long int msg_size) mutable {
            // cout << "In stability callback; subgroup = " << subgroup_num
            //      << "sender = " << sender_id
            //      << ", index = " << index << endl;

            int sender_rank = (sender_id - subgroup_num + num_nodes) % num_nodes;
            received_message_indices[subgroup_to_local_index.at(subgroup_num)][sender_rank] = index;
        };

        auto membership_function = [num_nodes, subgroup_size](const View &curr_view, int &next_unassigned_rank, bool previous_was_successful) {
            auto num_members = curr_view.members.size();
            if(num_members < num_nodes) {
                throw derecho::subgroup_provisioning_exception();
            }
            subgroup_shard_layout_t subgroup_vector(num_members);
            for(uint i = 0; i < num_members; ++i) {
                vector<uint32_t> members(subgroup_size);
                for(uint j = 0; j < subgroup_size; ++j) {
                    members[j] = (i + j) % num_members;
                }
                subgroup_vector[i].emplace_back(curr_view.make_subview(members));
            }
            next_unassigned_rank = curr_view.members.size();

            return subgroup_vector;
        };

        std::map<std::type_index, shard_view_generator_t> subgroup_map = {{std::type_index(typeid(RawObject)), membership_function}};
        derecho::SubgroupInfo raw_groups(subgroup_map);

        std::unique_ptr<derecho::Group<>> managed_group;
        if(node_id == server_rank) {
            managed_group = std::make_unique<derecho::Group<>>(
                    node_id, node_addresses[node_id],
                    derecho::CallbackSet{stability_callback, nullptr},
                    raw_groups,
                    derecho::DerechoParams{max_msg_size, block_size, std::string(), window_size});
        } else {
            managed_group = std::make_unique<derecho::Group<>>(
                    node_id, node_addresses[node_id],
                    node_addresses[server_rank],
                    derecho::CallbackSet{stability_callback, nullptr},
                    raw_groups);
        }

        cout << "Finished constructing/joining ManagedGroup" << endl;

        while(managed_group->get_members().size() < num_nodes) {
        }
        uint32_t node_rank = -1;
        auto members_order = managed_group->get_members();
        cout << "The order of members is :" << endl;
        for(uint i = 0; i < num_nodes; ++i) {
            cout << members_order[i] << " ";
            if(members_order[i] == node_id) {
                node_rank = i;
            }
        }
        cout << endl;

        vector<RawSubgroup> subgroups;
        for(uint i = 0; i < subgroup_size; ++i) {
            subgroups.emplace_back(managed_group->get_subgroup<RawObject>((node_id - i + num_nodes) % num_nodes));
        }
        // RawSubgroup &subgroup1 = managed_group->get_subgroup<RawObject>(node_id);
        // RawSubgroup &subgroup2 = managed_group->get_subgroup<RawObject>((node_id - 1 + num_nodes) % num_nodes);
        auto send_all = [&]() {
            for(uint i = 0; i < subgroup_size * num_messages; ++i) {
                uint j = i % subgroup_size;
                // cout << "Asking for a buffer" << endl;
                char *buf = subgroups[j].get_sendbuffer_ptr(max_msg_size, send_medium);
                while(!buf) {
                    buf = subgroups[j].get_sendbuffer_ptr(max_msg_size, send_medium);
                }
                buf[0] = '0' + i;
                // cout << "Obtained a buffer, sending" << endl;
                subgroups[j].send();
            }
        };
        // auto send_one = [&](uint32_t subgroup_num) {
        //     if(subgroup_num == 1) {
        //         for(uint i = 0; i < subgroup_size; ++i) {
        //             // cout << "Asking for a buffer" << endl;
        //             char *buf = subgroup1.get_sendbuffer_ptr(max_msg_size, send_medium);
        //             while(!buf) {
        //                 buf = subgroup1.get_sendbuffer_ptr(max_msg_size, send_medium);
        //             }
        //             buf[0] = '0' + i;
        //             // cout << "Obtained a buffer, sending" << endl;
        //             subgroup1.send();
        //         }
        //     } else {
        //         for(uint i = 0; i < subgroup_size; ++i) {
        //             // cout << "Asking for a buffer" << endl;
        //             char *buf = subgroup2.get_sendbuffer_ptr(max_msg_size, send_medium);
        //             while(!buf) {
        //                 buf = subgroup2.get_sendbuffer_ptr(max_msg_size, send_medium);
        //             }
        //             buf[0] = '0' + i;
        //             // cout << "Obtained a buffer, sending" << endl;
        //             subgroup2.send();
        //         }
        //     }
        // }

        struct timespec start_time;
        // start timer
        clock_gettime(CLOCK_REALTIME, &start_time);
        send_all();
        while(true) {
            bool all_done = true;
            for(uint i = 0; i < subgroup_size; ++i) {
                for(uint j = 0; j < subgroup_size; ++j) {
                    if(received_message_indices[i][j] < num_messages - 1) {
                        all_done = false;
                    }
                }
            }
            if(all_done) {
                break;
            }
        }

        struct timespec end_time;
        clock_gettime(CLOCK_REALTIME, &end_time);
        long long int nanoseconds_elapsed = (end_time.tv_sec - start_time.tv_sec) * (long long int)1e9 + (end_time.tv_nsec - start_time.tv_nsec);
        double bw;
        bw = (max_msg_size * num_messages * num_nodes * subgroup_size + 0.0) / nanoseconds_elapsed;
        double avg_bw = aggregate_bandwidth(members, node_id, bw);
        if(node_rank == 0) {
            log_results(exp_result{num_nodes, max_msg_size, subgroup_size, avg_bw},
                        "data_subgroup_scaling");
        }

        managed_group->barrier_sync();
        // managed_group->leave();
        // sst::verbs_destroy();
        exit(0);
        cout << "Finished destroying managed_group" << endl;
        std::this_thread::sleep_for(std::chrono::seconds(10));
    } catch(const std::exception &e) {
        cout << "Exception in main: " << e.what() << endl;
        cout << "main shutting down" << endl;
    }
}
