#include <iostream>

#include "block_size.h"
#include "derecho/derecho.h"
#include "rdmc/util.h"
#include "conf/conf.hpp"

using std::cin;
using std::cout;
using std::endl;
using std::string;

struct test1_str {
    int state;
    int read_state() {
        // cout << "In the read state function" << endl;
        return state;
    }
    bool change_state(int new_state) {
        state = new_state;
        return true;
    }

    REGISTER_RPC_FUNCTIONS(test1_str, read_state, change_state);
};

template <typename T>
void output_result(typename derecho::rpc::QueryResults<T>::ReplyMap& rmap) {
    // cout << "Obtained a reply map" << endl;
    for(auto it = rmap.begin(); it != rmap.end(); ++it) {
        it->second.get();
        // try {
        //     cout << "Reply from node " << it->first << ": " << it->second.get()
        //          << endl;
        // } catch(const std::exception& e) {
        //     cout << e.what() << endl;
        // }
    }
    // cout << "Returning from output_result" << endl;
}

int main() {
    string leader_ip;
    uint32_t my_id;
    string my_ip;
    cout << "Enter my id" << endl;
    cin >> my_id;
    cout << "Enter my ip" << endl;
    cin >> my_ip;
    cout << "Enter leader ip" << endl;
    cin >> leader_ip;

    long long unsigned int max_msg_size = 100;
    long long unsigned int block_size = get_block_size(max_msg_size);
    const long long unsigned int sst_max_msg_size = (max_msg_size < 17000 ? max_msg_size : 0);
    // int num_messages = 10;

    // auto stability_callback = [](uint32_t subgroup_num, int sender_id, long long int index, char* buf,
    //                              long long int msg_size) {};

    derecho::DerechoParams derecho_params{max_msg_size, sst_max_msg_size, block_size};
    derecho::SubgroupInfo subgroup_info{{{std::type_index(typeid(test1_str)), &derecho::one_subgroup_entire_view}},
                                        {std::type_index(typeid(test1_str))}};
    derecho::Group<test1_str>* managed_group;

    if(my_id == 0) {
        managed_group = new derecho::Group<test1_str>(
                my_id, my_ip, {{}, {}}, subgroup_info,
                derecho_params, {}, derecho::getConfInt32(CONF_DERECHO_GMS_PORT),
                [](PersistentRegistry* pr) { return std::make_unique<test1_str>(); });
        derecho::Replicated<test1_str>& rpc_handle = managed_group->get_subgroup<test1_str>(0);

        struct timespec start_time;
        // start timer
        clock_gettime(CLOCK_REALTIME, &start_time);

        long long int num_times = 10000;
        for(long long int i = 0; i < num_times; ++i) {
            // cout << "i = " << i << endl;
            try {
                output_result<int>(rpc_handle.p2p_query<RPC_NAME(read_state)>(1).get());
            } catch(...) {
                // cout << i << endl;
                // cout << "Exception thrown" << endl;
                // exit(0);
            }
        }

        struct timespec end_time;
        clock_gettime(CLOCK_REALTIME, &end_time);
        long long int nanoseconds_elapsed = (end_time.tv_sec - start_time.tv_sec) * (long long int)1e9 + (end_time.tv_nsec - start_time.tv_nsec);
        cout << "Average latency: " << (double)nanoseconds_elapsed / (num_times * 1000) << " microseconds" << endl;
    } else {
        managed_group = new derecho::Group<test1_str>(
                my_id, my_ip, leader_ip,
                {{}, {}}, subgroup_info,
                {}, derecho::getConfInt32(CONF_DERECHO_GMS_PORT),
                [](PersistentRegistry* pr) { return std::make_unique<test1_str>(); });
    }
    managed_group->barrier_sync();
}
