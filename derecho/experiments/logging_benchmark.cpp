/**
 * @file logging_benchmark.cpp
 *
 * @date Mar 23, 2017
 * @author edward
 */

#include <chrono>
#include <iostream>

#include "derecho/derecho.h"
#include "initialize.h"

using namespace std::chrono;

int main(int argc, char** argv) {
    derecho::node_id_t node_id;
    derecho::ip_addr my_ip;
    derecho::ip_addr leader_ip;

    query_node_info(node_id, my_ip, leader_ip);

    //Where do these come from? What do they mean? Does the user really need to supply them?
    long long unsigned int max_msg_size = 100;
    long long unsigned int block_size = 100000;
    derecho::DerechoParams derecho_params{max_msg_size, block_size};

    derecho::message_callback stability_callback{};
    derecho::CallbackSet callback_set{stability_callback, {}};
    derecho::SubgroupInfo one_raw_group{{{std::type_index(typeid(derecho::RawObject)), &derecho::one_subgroup_entire_view}}};

    std::unique_ptr<derecho::Group<>> group;
    if(my_ip == leader_ip) {
        group = std::make_unique<derecho::Group<>>(
            my_ip, callback_set, one_raw_group, derecho_params);
    } else {
        group = std::make_unique<derecho::Group<>>(
            node_id, my_ip, leader_ip, callback_set, one_raw_group);
    }

    int test_iterations = 1000000;
    auto start_time = high_resolution_clock::now();
    for(int c = 0; c < test_iterations; ++c) {
        group->debug_log().debug("Testing logging with message {}", c);
    }
    auto end_time = high_resolution_clock::now();
    nanoseconds elapsed_time = duration_cast<nanoseconds>(end_time - start_time);
    std::cout << "Took " << elapsed_time.count() << " nanoseconds to log " << test_iterations << " messages" << std::endl;
    std::cout << (elapsed_time / test_iterations).count() << " nanoseconds per messsage" << std::endl;
}
