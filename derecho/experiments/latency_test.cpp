#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include "rdmc/rdmc.h"
#include "rdmc/util.h"

#include "block_size.h"
#include "derecho/derecho.h"
#include "log_results.h"

using namespace std;

unique_ptr<rdmc::barrier_group> universal_barrier_group;

void query_node_info(derecho::node_id_t &node_id, derecho::ip_addr &node_ip, derecho::ip_addr &leader_ip) {
    cout << "Please enter this node's ID: ";
    cin >> node_id;
    cout << "Please enter this node's IP address: ";
    cin >> node_ip;
    cout << "Please enter the leader node's IP address: ";
    cin >> leader_ip;
}

struct exp_result {
    uint32_t num_nodes;
    long long unsigned int max_msg_size;
    unsigned int window_size;
    int num_messages;
    // int send_medium;
    int raw_mode;
    double latency;
    double stddev;

    void print(std::ofstream &fout) {
        fout << num_nodes << " " << max_msg_size
	     << " " << window_size << " "
             // << num_messages << " " << send_medium << " "
             << num_messages << " "
             << raw_mode << " " << latency << " "
             << stddev << endl;
    }
};

int main(int argc, char *argv[]) {
    try {
        if(argc < 5) {
            cout << "Insufficient number of command line arguments" << endl;
            cout << "Enter num_nodes, msg_size, window_size, raw_mode" << endl;
            cout << "Thank you" << endl;
            exit(1);
        }
        uint32_t num_nodes = std::atoi(argv[1]);
        unsigned int msg_size = std::atoi(argv[2]);
        derecho::node_id_t node_id;
        derecho::ip_addr my_ip;
        derecho::node_id_t leader_id = 0;
        derecho::ip_addr leader_ip;

        query_node_info(node_id, my_ip, leader_ip);

        const long long unsigned int max_msg_size = msg_size;
        const long long unsigned int block_size = get_block_size(max_msg_size);
        const long long unsigned int sst_max_msg_size = (max_msg_size < 17000 ? max_msg_size : 0);
        const unsigned int window_size = atoi(argv[3]);
        const int raw_mode = atoi(argv[4]);

        int num_messages = 1000;
	// only used by node 0
        vector<uint64_t> start_times(num_messages), end_times(num_messages);

        volatile bool done = false;
        auto stability_callback = [&num_messages, &done, &num_nodes, &end_times](
                                          int32_t subgroup, int sender_id, long long int index, char *buf,
                                          long long int msg_size) mutable {
            // cout << buf << endl;
            // cout << "Delivered a message" << endl;
            DERECHO_LOG(sender_id, index, "complete_send");
            if(sender_id == 0) {
                end_times[index] = get_time();
            }
            if(index == num_messages - 1 && sender_id == (int)num_nodes - 1) {
                done = true;
            }
        };

        unique_ptr<derecho::SubgroupInfo> one_raw_group;
        std::map<std::type_index, derecho::shard_view_generator_t> membership_map;
        if(raw_mode) {
            membership_map = {{std::type_index(typeid(derecho::RawObject)), derecho::one_subgroup_entire_view_raw}};
            one_raw_group = make_unique<derecho::SubgroupInfo>(membership_map);
        } else {
            membership_map = {{std::type_index(typeid(derecho::RawObject)), derecho::one_subgroup_entire_view}};
            one_raw_group = make_unique<derecho::SubgroupInfo>(membership_map);
        }

        derecho::CallbackSet callbacks{stability_callback, nullptr};
        derecho::DerechoParams param_object{max_msg_size, sst_max_msg_size, block_size, window_size};
        std::unique_ptr<derecho::Group<>> managed_group;

        if(node_id == leader_id) {
            managed_group = std::make_unique<derecho::Group<>>(
                    node_id, my_ip,
                    callbacks,
                    *one_raw_group,
                    param_object);
        } else {
            managed_group = std::make_unique<derecho::Group<>>(
                    node_id, my_ip,
                    leader_ip,
                    callbacks,
                    *one_raw_group);
        }

        while(managed_group->get_members().size() < num_nodes) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        std::cout<<"All nodes joined."<<std::endl;

        int my_rank = 0;
        auto group_members = managed_group->get_members();
        while(group_members[my_rank] != node_id) my_rank++;

        vector<uint32_t> members;
        for(uint32_t i = 0; i < num_nodes; i++) members.push_back(i);
        universal_barrier_group = std::make_unique<rdmc::barrier_group>(members);

        universal_barrier_group->barrier_wait();
        uint64_t t1 = get_time();
        universal_barrier_group->barrier_wait();
        uint64_t t2 = get_time();
        reset_epoch();
        universal_barrier_group->barrier_wait();
        uint64_t t3 = get_time();
        printf(
                "Synchronized clocks.\nTotal possible variation = %5.3f us\n"
                "Max possible variation from local = %5.3f us\n",
                (t3 - t1) * 1e-3f, max(t2 - t1, t3 - t2) * 1e-3f);
        fflush(stdout);

        if(node_id == 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }

        derecho::RawSubgroup &group_as_subgroup = managed_group->get_subgroup<derecho::RawObject>();
        for(int i = 0; i < num_messages; ++i) {
            char *buf = group_as_subgroup.get_sendbuffer_ptr(msg_size);
            while(!buf) {
                buf = group_as_subgroup.get_sendbuffer_ptr(msg_size);
            }
            for(unsigned int j = 0; j < msg_size - 1; ++j) {
                buf[j] = 'a' + (i % 26);
            }
            buf[msg_size - 1] = 0;
            start_times[i] = get_time();
            DERECHO_LOG(my_rank, i, "start_send");
            group_as_subgroup.send();

            if(node_id == 0) {
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
        }
        while(!done) {
        }

        uint64_t total_time = 0;
        double sum_of_square = 0.0f;
        double average_time = 0.0f;
        for(int i = 0; i < num_messages; ++i) {
            total_time += end_times[i] - start_times[i];
            // std::cout << ((end_times[i] - start_times[i])/1000.0) << "us" << std::endl;
        }
        average_time = (total_time/num_messages); // in nano seconds
        // calculate the standard deviation:
        for (int i=0; i < num_messages; ++i) {
          sum_of_square += (double)(end_times[i] - start_times[i] - average_time) * (end_times[i] - start_times[i] - average_time) ;
        }
        double std = sqrt(sum_of_square / (num_messages - 1));

        if(node_id == 0) {
	    log_results(exp_result{num_nodes, max_msg_size, window_size, num_messages, raw_mode, (average_time / 1000.0), (std / 1000.0)}, "data_latency");
        }
        managed_group->barrier_sync();
        flush_events();
        // for(int i = 100; i < num_messages - 100; i+= 5){
        // 	printf("%5.3f\n", (end_times[my_rank][i] - start_times[i]) * 1e-3);
        // }

        managed_group->barrier_sync();
        // managed_group->leave();
        // sst::verbs_destroy();
        exit(0);
    } catch(const std::exception &e) {
        cout << "Main got an exception: " << e.what() << endl;
        throw e;
    }

    cout << "Finished destroying managed_group" << endl;
}
