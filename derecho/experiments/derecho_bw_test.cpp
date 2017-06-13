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
    uint num_senders_selector;
    long long unsigned int max_msg_size;
    unsigned int window_size;
    int num_messages;
    int send_medium;
    int raw_mode;
    double bw;

    void print(std::ofstream &fout) {
        fout << num_nodes << " " << num_senders_selector << " "
             << max_msg_size << " " << window_size << " "
             << num_messages << " " << send_medium << " "
             << raw_mode << " " << bw << endl;
    }
};

int main(int argc, char *argv[]) {
    try {
        if(argc < 7) {
            cout << "Insufficient number of command line arguments" << endl;
            cout << "Enter max_msg_size, num_senders_selector, window_size, num_messages, send_medium, raw_mode" << endl;
            cout << "Thank you" << endl;
            exit(1);
        }
        pthread_setname_np(pthread_self(), "bw_test");
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
        const uint num_senders_selector = atoi(argv[2]);
        const unsigned int window_size = atoi(argv[3]);
        const int num_messages = atoi(argv[4]);
        const int send_medium = atoi(argv[5]);
        const int raw_mode = atoi(argv[6]);

        volatile bool done = false;
        auto stability_callback = [
            &num_messages,
            &done,
            &num_nodes,
            num_senders_selector,
            num_last_received = 0u
        ](uint32_t subgroup, int sender_id, long long int index, char *buf, long long int msg_size) mutable {
            // cout << "In stability callback; sender = " << sender_id
            // << ", index = " << index << endl;
            if(num_senders_selector == 0) {
                if(index == num_messages - 1 && sender_id == (int)num_nodes - 1) {
                    done = true;
                }
            } else if(num_senders_selector == 1) {
                if(index == num_messages - 1) {
                    ++num_last_received;
                }
                if(num_last_received == num_nodes / 2) {
                    done = true;
                }
            } else {
                if(index == num_messages - 1) {
                    done = true;
                }
            }
        };

        derecho::Mode mode = derecho::Mode::ORDERED;
        if(raw_mode) {
            mode = derecho::Mode::RAW;
        }

        auto membership_function = [num_senders_selector, mode, num_nodes](const View &curr_view, int &next_unassigned_rank, bool previous_was_successful) {
            subgroup_shard_layout_t subgroup_vector(1);
            auto num_members = curr_view.members.size();
            if(num_members < num_nodes) {
                throw derecho::subgroup_provisioning_exception();
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
        derecho::SubgroupInfo one_raw_group(subgroup_map);

        std::unique_ptr<derecho::Group<>> managed_group;
        if(node_id == server_rank) {
            managed_group = std::make_unique<derecho::Group<>>(
                    node_id, node_addresses[node_id],
                    derecho::CallbackSet{stability_callback, nullptr},
                    one_raw_group,
                    derecho::DerechoParams{max_msg_size, block_size, std::string(), window_size});
        } else {
            managed_group = std::make_unique<derecho::Group<>>(
                    node_id, node_addresses[node_id],
                    node_addresses[server_rank],
                    derecho::CallbackSet{stability_callback, nullptr},
                    one_raw_group);
        }

        cout << "Finished constructing/joining ManagedGroup" << endl;

        while(managed_group->get_members().size() < num_nodes) {
        }
	uint32_t node_rank = -1;
        auto members_order = managed_group->get_members();
        cout << "The order of members is :" << endl;
        for(uint i = 0; i < num_nodes; ++i) {
            cout << members_order[i] << " ";
	    if (members_order[i] == node_id) {
	      node_rank = i;
	    }
        }
        cout << endl;

        auto send_all = [&]() {
            RawSubgroup &group_as_subgroup = managed_group->get_subgroup<RawObject>();
            for(int i = 0; i < num_messages; ++i) {
                // cout << "Asking for a buffer" << endl;
                char *buf = group_as_subgroup.get_sendbuffer_ptr(max_msg_size, send_medium);
                while(!buf) {
                    buf = group_as_subgroup.get_sendbuffer_ptr(max_msg_size, send_medium);
                }
                buf[0] = '0' + i;
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
        double avg_bw = aggregate_bandwidth(members, node_rank, bw);
        if(node_rank == 0) {
            log_results(exp_result{num_nodes, num_senders_selector, max_msg_size,
                                   window_size, num_messages, send_medium,
                                   raw_mode, avg_bw},
                        "data_derecho_bw");
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
