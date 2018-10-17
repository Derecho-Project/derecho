/**
 * @file repeated_rpc_test.cpp
 *
 * @date Oct 11, 2017
 * @author edward
 */

#include <iostream>

#include "derecho/derecho.h"
#include "initialize.h"
#include "test_objects.h"
#include "conf/conf.hpp"

using derecho::ExternalCaller;
using derecho::Replicated;
using std::cout;
using std::endl;

int main(int argc, char** argv) {
    derecho::node_id_t node_id;
    derecho::ip_addr my_ip;
    derecho::ip_addr leader_ip;

    query_node_info(node_id, my_ip, leader_ip);

    //Derecho message parameters
    //Where do these come from? What do they mean? Does the user really need to supply them?
    long long unsigned int max_msg_size = 100;
    long long unsigned int block_size = 100000;
    const long long unsigned int sst_max_msg_size = (max_msg_size < 17000 ? max_msg_size : 0);
    derecho::DerechoParams derecho_params{max_msg_size, sst_max_msg_size, block_size};

    derecho::message_callback_t stability_callback{};
    derecho::CallbackSet callback_set{stability_callback};

    const int num_nodes_in_test = 3;
    derecho::SubgroupInfo subgroup_info{
            {{std::type_index(typeid(Foo)), [](const derecho::View& curr_view, int& next_unassigned_rank) {
                  if(curr_view.num_members < num_nodes_in_test) {
                      std::cout << "Foo function throwing subgroup_provisioning_exception" << std::endl;
                      throw derecho::subgroup_provisioning_exception();
                  }
                  derecho::subgroup_shard_layout_t subgroup_vector(1);
                  std::vector<derecho::node_id_t> node_list(&curr_view.members[0], &curr_view.members[0] + num_nodes_in_test);
                  //Put the desired SubView at subgroup_vector[0][0] since there's one subgroup with one shard
                  subgroup_vector[0].emplace_back(curr_view.make_subview(node_list));
                  next_unassigned_rank = std::max(next_unassigned_rank, num_nodes_in_test);
                  cout << "Foo function setting next_unassigned_rank to " << next_unassigned_rank << endl;
                  return subgroup_vector;
              }}},
            {std::type_index(typeid(Foo))}};

    auto foo_factory = [](PersistentRegistry*) { return std::make_unique<Foo>(-1); };

    std::unique_ptr<derecho::Group<Foo>> group;
    if(my_ip == leader_ip) {
        group = std::make_unique<derecho::Group<Foo>>(
                node_id, my_ip, callback_set, subgroup_info, derecho_params,
                std::vector<derecho::view_upcall_t>{}, derecho::getConfInt32(CONF_DERECHO_GMS_PORT),
                foo_factory);
    } else {
        group = std::make_unique<derecho::Group<Foo>>(
                node_id, my_ip, leader_ip, callback_set, subgroup_info,
                std::vector<derecho::view_upcall_t>{}, derecho::getConfInt32(CONF_DERECHO_GMS_PORT),
                foo_factory);
    }

    cout << "Finished constructing/joining Group" << endl;

    Replicated<Foo>& foo_rpc_handle = group->get_subgroup<Foo>();
    int trials = 1000;
    cout << "Changing Foo's state " << trials << " times" << endl;
    for(int count = 0; count < trials; ++count) {
        cout << "Sending query #" << count << std::endl;
        derecho::rpc::QueryResults<bool> results = foo_rpc_handle.ordered_query<RPC_NAME(change_state)>(count);
        bool results_total = true;
        for(auto& reply_pair : results.get()) {
            cout << "Waiting for results from " << reply_pair.first << endl;
            results_total = results_total && reply_pair.second.get();
        }
    }

    cout << "Reached end of main()" << endl;
    group->barrier_sync();
    return 0;
}
