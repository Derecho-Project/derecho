/**
 * @file simple_replicated_objects.cpp
 *
 * This test creates two subgroups, one of each type Foo and Bar (defined in sample_objects.h).
 * It requires at least 6 nodes to join the group; the first three are part of the Foo subgroup,
 * while the next three are part of the Bar subgroup.
 * Every node (identified by its node_id) makes some calls to ordered_send in their subgroup;
 * some also call p2p_send. By these calls they verify that the state machine operations are
 * executed properly.
 */
#include <cerrno>
#include <cstdlib>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <derecho/conf/conf.hpp>
#include <derecho/core/derecho.hpp>
#include "sample_objects.hpp"

using derecho::PeerCaller;
using derecho::Replicated;
using std::cout;
using std::endl;

int main(int argc, char** argv) {
    // Read configurations from the command line options as well as the default config file
    derecho::Conf::initialize(argc, argv);

    //Define subgroup membership using the default subgroup allocator function
    //Each Replicated type will have one subgroup and one shard, with three members in the shard
    derecho::SubgroupInfo subgroup_function {derecho::DefaultSubgroupAllocator({
        {std::type_index(typeid(Foo)), derecho::one_subgroup_policy(derecho::fixed_even_shards(1,3))},
        {std::type_index(typeid(Bar)), derecho::one_subgroup_policy(derecho::fixed_even_shards(1,3))}
    })};
    //Each replicated type needs a factory; this can be used to supply constructor arguments
    //for the subgroup's initial state. These must take a PersistentRegistry* argument, but
    //in this case we ignore it because the replicated objects aren't persistent.
    auto foo_factory = [](persistent::PersistentRegistry*,derecho::subgroup_id_t) { return std::make_unique<Foo>(-1); };
    auto bar_factory = [](persistent::PersistentRegistry*,derecho::subgroup_id_t) { return std::make_unique<Bar>(); };

    derecho::Group<Foo, Bar> group(derecho::UserMessageCallbacks{}, subgroup_function, {},
                                          std::vector<derecho::view_upcall_t>{},
                                          foo_factory, bar_factory);

    cout << "Finished constructing/joining Group" << endl;

    uint32_t my_id = derecho::getConfUInt32(CONF_DERECHO_LOCAL_ID);
    //Now have each node send some updates to the Replicated objects.
    //The code must be different depending on which subgroup this node is in,
    //which we can determine by attempting to locate its shard in each subgroup.
    //Note that since there is only one subgroup of each type we can omit the
    //subgroup_index parameter to many of the group.get_X() methods
    int32_t my_foo_shard = group.get_my_shard<Foo>();
    int32_t my_bar_shard = group.get_my_shard<Bar>();
    if(my_foo_shard != -1) {
        std::vector<node_id_t> foo_members = group.get_subgroup_members<Foo>()[my_foo_shard];
        uint32_t rank_in_foo = derecho::index_of(foo_members, my_id);
        Replicated<Foo>& foo_rpc_handle = group.get_subgroup<Foo>();
        //Each member within the shard sends a different multicast
        if(rank_in_foo == 0) {
            int new_value = 1;
            cout << "Changing Foo's state to " << new_value << endl;
            derecho::rpc::QueryResults<bool> results = foo_rpc_handle.ordered_send<RPC_NAME(change_state)>(new_value);
            decltype(results)::ReplyMap& replies = results.get();
            cout << "Got a reply map!" << endl;
            for(auto& reply_pair : replies) {
                cout << "Reply from node " << reply_pair.first << " was " << std::boolalpha << reply_pair.second.get() << endl;
            }
            cout << "Reading Foo's state just to allow node 1's message to be delivered" << endl;
            foo_rpc_handle.ordered_send<RPC_NAME(read_state)>();
        } else if(rank_in_foo == 1) {
            int new_value = 3;
            cout << "Changing Foo's state to " << new_value << endl;
            derecho::rpc::QueryResults<bool> results = foo_rpc_handle.ordered_send<RPC_NAME(change_state)>(new_value);
            decltype(results)::ReplyMap& replies = results.get();
            cout << "Got a reply map!" << endl;
            for(auto& reply_pair : replies) {
                cout << "Reply from node " << reply_pair.first << " was " << std::boolalpha << reply_pair.second.get() << endl;
            }
        } else if(rank_in_foo == 2) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            cout << "Reading Foo's state from the group" << endl;
            derecho::rpc::QueryResults<int> foo_results = foo_rpc_handle.ordered_send<RPC_NAME(read_state)>();
            for(auto& reply_pair : foo_results.get()) {
                cout << "Node " << reply_pair.first << " says the state is: " << reply_pair.second.get() << endl;
            }
        }
    } else if(my_bar_shard != -1) {
        std::vector<node_id_t> bar_members = group.get_subgroup_members<Bar>()[my_bar_shard];
        uint32_t rank_in_bar = derecho::index_of(bar_members, my_id);
        Replicated<Bar>& bar_rpc_handle = group.get_subgroup<Bar>();
        if(rank_in_bar == 0) {
            cout << "Appending to Bar." << endl;
            derecho::rpc::QueryResults<void> void_future = bar_rpc_handle.ordered_send<RPC_NAME(append)>("Write from 0...");
            derecho::rpc::QueryResults<void>::ReplyMap& sent_nodes = void_future.get();
            cout << "Append delivered to nodes: ";
            for(const node_id_t& node : sent_nodes) {
                cout << node << " ";
            }
            cout << endl;
        } else if(rank_in_bar == 1) {
            cout << "Appending to Bar" << endl;
            bar_rpc_handle.ordered_send<RPC_NAME(append)>("Write from 1...");
            //Send to node rank 2 in shard 0 of the Foo subgroup
            node_id_t p2p_target = group.get_subgroup_members<Foo>()[0][2];
            cout << "Reading Foo's state from node " << p2p_target << endl;
            PeerCaller<Foo>& p2p_foo_handle = group.get_nonmember_subgroup<Foo>();
            derecho::rpc::QueryResults<int> foo_results = p2p_foo_handle.p2p_send<RPC_NAME(read_state)>(p2p_target);
            int response = foo_results.get().get(p2p_target);
            cout << "  Response: " << response << endl;
        } else if(rank_in_bar == 2) {
            bar_rpc_handle.ordered_send<RPC_NAME(append)>("Write from 2...");
            cout << "Printing log from Bar" << endl;
            derecho::rpc::QueryResults<std::string> bar_results = bar_rpc_handle.ordered_send<RPC_NAME(print)>();
            for(auto& reply_pair : bar_results.get()) {
                cout << "Node " << reply_pair.first << " says the log is: " << reply_pair.second.get() << endl;
            }
            cout << "Clearing Bar's log" << endl;
            derecho::rpc::QueryResults<void> void_future = bar_rpc_handle.ordered_send<RPC_NAME(clear)>();
        }
    } else {
        std::cout << "This node was not assigned to any subgroup!" << std::endl;
    }

    cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
    while(true) {
    }
}
