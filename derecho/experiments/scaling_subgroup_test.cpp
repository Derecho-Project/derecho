#include <iostream>
#include <memory>
#include <thread>
#include <typeindex>

#include "derecho/derecho.h"
#include "initialize.h"
#include "test_objects.h"

using std::cout;
using std::endl;
using derecho::Replicated;
using derecho::ExternalCaller;

int main(int argc, char** argv) {
    derecho::node_id_t node_id;
    derecho::ip_addr my_ip;
    derecho::ip_addr leader_ip;

    query_node_info(node_id, my_ip, leader_ip);

    //Derecho message parameters
    long long unsigned int max_msg_size = 100;
    long long unsigned int block_size = 100000;
    derecho::DerechoParams derecho_params{max_msg_size, block_size};

    derecho::message_callback_t stability_callback{};
    derecho::CallbackSet callback_set{stability_callback, {}};

    derecho::SubgroupInfo subgroup_info{
        {{std::type_index(typeid(Foo)), derecho::DefaultSubgroupAllocator(derecho::one_subgroup_policy(derecho::even_sharding_policy(1,3)))},
         {std::type_index(typeid(Bar)), [](const derecho::View& curr_view, int& next_unassigned_rank) {
             if(curr_view.num_members - next_unassigned_rank < 3) {
                 throw derecho::subgroup_provisioning_exception();
             }
             derecho::subgroup_shard_layout_t subgroup_vector(1);
             std::vector<derecho::node_id_t> first_3_nodes(&curr_view.members[next_unassigned_rank],
                                                           &curr_view.members[next_unassigned_rank] + 3);
             subgroup_vector[0].emplace_back(curr_view.make_subview(first_3_nodes));
             next_unassigned_rank += 3;
             //If there are at least 3 more nodes left, make a second subgroup
             if(curr_view.num_members - next_unassigned_rank >= 3) {
                 std::vector<derecho::node_id_t> next_3_nodes(&curr_view.members[next_unassigned_rank],
                                                              &curr_view.members[next_unassigned_rank] + 3);
                 subgroup_vector.emplace_back(std::vector<derecho::SubView>{curr_view.make_subview(next_3_nodes)});
                 next_unassigned_rank += 3;
             }
             return subgroup_vector;
         }}},
        {std::type_index(typeid(Foo)), std::type_index(typeid(Bar))}};

    auto foo_factory = [](PersistentRegistry *) { return std::make_unique<Foo>(-1); };
    auto bar_factory = [](PersistentRegistry *) { return std::make_unique<Bar>(); };

    std::unique_ptr<derecho::Group<Foo, Bar>> group;
    if(my_ip == leader_ip) {
        group = std::make_unique<derecho::Group<Foo, Bar>>(
                node_id, my_ip, callback_set, subgroup_info, derecho_params,
                std::vector<derecho::view_upcall_t>{}, derecho::derecho_gms_port,
                foo_factory, bar_factory);
    } else {
        group = std::make_unique<derecho::Group<Foo, Bar>>(
                node_id, my_ip, leader_ip, callback_set, subgroup_info,
                std::vector<derecho::view_upcall_t>{}, derecho::derecho_gms_port,
                foo_factory, bar_factory);
    }

    cout << "Finished constructing/joining Group" << endl;

    if(node_id == 0) {
        Replicated<Foo>& foo_rpc_handle = group->get_subgroup<Foo>();
        ExternalCaller<Bar>& bar_rpc_handle = group->get_nonmember_subgroup<Bar>();
        foo_rpc_handle.ordered_query<RPC_NAME(change_state)>(0);
        cout << "Reading Foo's state from the group" << endl;
        derecho::rpc::QueryResults<int> foo_results = foo_rpc_handle.ordered_query<RPC_NAME(read_state)>();
        for(auto& reply_pair : foo_results.get()) {
            cout << "Node " << reply_pair.first << " says the state is: " << reply_pair.second.get() << endl;
        }
        cout << endl;
        int p2p_target = 3;
        derecho::rpc::QueryResults<std::string> bar_results = bar_rpc_handle.p2p_query<RPC_NAME(print)>(p2p_target);
        std::string response = bar_results.get().get(p2p_target);
        cout << "Node " << p2p_target << "'s state for Bar: " << response << endl;
    }
    if(node_id == 1) {
        Replicated<Foo>& foo_rpc_handle = group->get_subgroup<Foo>();
        foo_rpc_handle.ordered_query<RPC_NAME(change_state)>(node_id);
        cout << "Reading Foo's state from the group" << endl;
        derecho::rpc::QueryResults<int> foo_results = foo_rpc_handle.ordered_query<RPC_NAME(read_state)>();
        for(auto& reply_pair : foo_results.get()) {
            cout << "Node " << reply_pair.first << " says the state is: " << reply_pair.second.get() << endl;
        }
        cout << endl;
    }
    if(node_id == 2) {
        Replicated<Foo>& foo_rpc_handle = group->get_subgroup<Foo>();
        ExternalCaller<Bar>& bar_rpc_handle = group->get_nonmember_subgroup<Bar>();
        foo_rpc_handle.ordered_query<RPC_NAME(change_state)>(node_id);
        cout << "Reading Foo's state from the group" << endl;
        derecho::rpc::QueryResults<int> foo_results = foo_rpc_handle.ordered_query<RPC_NAME(read_state)>();
        for(auto& reply_pair : foo_results.get()) {
            cout << "Node " << reply_pair.first << " says the state is: " << reply_pair.second.get() << endl;
        }
        cout << endl;
        int p2p_target = 4;
        derecho::rpc::QueryResults<std::string> bar_results = bar_rpc_handle.p2p_query<RPC_NAME(print)>(p2p_target);
        std::string response = bar_results.get().get(p2p_target);
        cout << "Node " << p2p_target << "'s state for Bar: " << response << endl;
    }
    if(node_id > 2 && node_id < 6) {
        Replicated<Bar>& bar_rpc_handle = group->get_subgroup<Bar>(0);
        ExternalCaller<Foo>& foo_p2p_handle = group->get_nonmember_subgroup<Foo>();
        cout << "Sending updates to Bar object, subgroup 0" << endl;
        for(int i = 0; i < 10; ++i) {
            std::stringstream text;
            text << "Node " << node_id << " update " << i;
            bar_rpc_handle.ordered_send<RPC_NAME(append)>(text.str());
        }
        derecho::rpc::QueryResults<std::string> bar_results = bar_rpc_handle.ordered_query<RPC_NAME(print)>();
        for(auto& reply_pair : bar_results.get()) {
            cout << "Node " << reply_pair.first << " says the log is: " << reply_pair.second.get() << endl;
        }
        int p2p_target = 1;
        derecho::rpc::QueryResults<int> foo_results = foo_p2p_handle.p2p_query<RPC_NAME(read_state)>(p2p_target);
        int response = foo_results.get().get(p2p_target);
        cout << "Node " << p2p_target << " says Foo's state is " << response << endl;
    }
    if(node_id > 5) {
        Replicated<Bar>& bar_rpc_handle = group->get_subgroup<Bar>(1);
        cout << "Sending updates to Bar object, subgroup 1" << endl;
        for(int i = 0; i < 10; ++i) {
            std::stringstream text;
            text << "Node " << node_id << " update " << i;
            bar_rpc_handle.ordered_send<RPC_NAME(append)>(text.str());
        }
        derecho::rpc::QueryResults<std::string> bar_results = bar_rpc_handle.ordered_query<RPC_NAME(print)>();
        for(auto& reply_pair : bar_results.get()) {
            cout << "Node " << reply_pair.first << " says the log is: " << reply_pair.second.get() << endl;
        }
    }

    cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
    while(true) {
    }
}
