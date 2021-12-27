/**
 * @file overlapping_replicated_objects.cpp
 *
 * This test creates three subgroups, one of each type Foo, Bar and Cache (defined in sample_objects.h).
 * It requires at least 6 nodes to join the group; the first three are part of subgroups of Foo and Bar
 * while the last three are part of Cache.
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

    //Define subgroup membership for each Replicated type
    //Each Replicated type will have one subgroup and one shard, with three members in the shard
    //The Foo and Bar subgroups will both reside on the first 3 nodes in the view,
    //while the Cache subgroup will reside on the second 3 nodes in the view.
    derecho::SubgroupInfo subgroup_info{[](
            const std::vector<std::type_index>& subgroup_type_order,
            const std::unique_ptr<derecho::View>& prev_view, derecho::View& curr_view) {
        derecho::subgroup_allocation_map_t subgroup_allocation;
        for(const auto& subgroup_type : subgroup_type_order) {
            derecho::subgroup_shard_layout_t subgroup_layout(1);
            if(subgroup_type == std::type_index(typeid(Foo)) || subgroup_type == std::type_index(typeid(Bar))) {
                // must have at least 3 nodes in the top-level group
                if(curr_view.num_members < 3) {
                    throw derecho::subgroup_provisioning_exception();
                }
                std::vector<node_id_t> first_3_nodes(&curr_view.members[0], &curr_view.members[0] + 3);
                //Put the desired SubView at subgroup_layout[0][0] since there's one subgroup with one shard
                subgroup_layout[0].emplace_back(curr_view.make_subview(first_3_nodes));
                //Advance next_unassigned_rank by 3, unless it was already beyond 3, since we assigned the first 3 nodes
                curr_view.next_unassigned_rank = std::max(curr_view.next_unassigned_rank, 3);
            } else { //subgroup_type == std::type_index(typeid(Cache))
                // must have at least 6 nodes in the top-level group
                if(curr_view.num_members < 6) {
                    throw derecho::subgroup_provisioning_exception();
                }
                std::vector<node_id_t> next_3_nodes(&curr_view.members[3], &curr_view.members[3] + 3);
                subgroup_layout[0].emplace_back(curr_view.make_subview(next_3_nodes));
                curr_view.next_unassigned_rank += 3;
            }
            subgroup_allocation.emplace(subgroup_type, std::move(subgroup_layout));
        }
        return subgroup_allocation;
    }};

    //Each replicated type needs a factory; this can be used to supply constructor arguments
    //for the subgroup's initial state. These must take a PersistentRegistry* argument, but
    //in this case we ignore it because the replicated objects aren't persistent.
    auto foo_factory = [](persistent::PersistentRegistry*,derecho::subgroup_id_t) { return std::make_unique<Foo>(-1); };
    auto bar_factory = [](persistent::PersistentRegistry*,derecho::subgroup_id_t) { return std::make_unique<Bar>(); };
    auto cache_factory = [](persistent::PersistentRegistry*,derecho::subgroup_id_t) { return std::make_unique<Cache>(); };

    derecho::Group<Foo, Bar, Cache> group(derecho::UserMessageCallbacks{}, subgroup_info, {},
                                          std::vector<derecho::view_upcall_t>{},
                                          foo_factory, bar_factory, cache_factory);

    cout << "Finished constructing/joining Group" << endl;

    uint32_t my_id = derecho::getConfUInt32(CONF_DERECHO_LOCAL_ID);
    //Now have each node send some updates to the Replicated objects
    //The code must be different depending on which subgroup this node is in,
    //which we can determine by attempting to locate its shard in each subgroup.
    //Note that there is only one subgroup of each type.
    int32_t my_foo_shard = group.get_my_shard<Foo>();
    int32_t my_cache_shard = group.get_my_shard<Cache>();
    if(my_foo_shard != -1) {
        std::vector<node_id_t> foo_members = group.get_subgroup_members<Foo>()[my_foo_shard];
        //Any node in subgroup foo is also in subgroup bar
        uint32_t rank_in_foo_and_bar = derecho::index_of(foo_members, my_id);
        Replicated<Foo>& foo_rpc_handle = group.get_subgroup<Foo>();
        Replicated<Bar>& bar_rpc_handle = group.get_subgroup<Bar>();
        if(rank_in_foo_and_bar == 0) {
            cout << "Appending to Bar." << endl;
            derecho::rpc::QueryResults<void> void_future = bar_rpc_handle.ordered_send<RPC_NAME(append)>("Write from 0...");
            derecho::rpc::QueryResults<void>::ReplyMap& sent_nodes = void_future.get();
            cout << "Append delivered to nodes: ";
            for(const node_id_t& node : sent_nodes) {
                cout << node << " ";
            }
            cout << endl;
            cout << "Reading Foo's state just to allow node 1's message to be delivered" << endl;
            foo_rpc_handle.ordered_send<RPC_NAME(read_state)>();
        } else if(rank_in_foo_and_bar == 1) {
            int new_value = 3;
            cout << "Changing Foo's state to " << new_value << endl;
            derecho::rpc::QueryResults<bool> results = foo_rpc_handle.ordered_send<RPC_NAME(change_state)>(new_value);
            decltype(results)::ReplyMap& replies = results.get();
            cout << "Got a reply map!" << endl;
            for(auto& reply_pair : replies) {
                cout << "Reply from node " << reply_pair.first << " was " << std::boolalpha << reply_pair.second.get() << endl;
            }
            cout << "Appending to Bar" << endl;
            bar_rpc_handle.ordered_send<RPC_NAME(append)>("Write from 1...");
        } else if(rank_in_foo_and_bar == 2) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            cout << "Reading Foo's state from the group" << endl;
            derecho::rpc::QueryResults<int> foo_results = foo_rpc_handle.ordered_send<RPC_NAME(read_state)>();
            for(auto& reply_pair : foo_results.get()) {
                cout << "Node " << reply_pair.first << " says the state is: " << reply_pair.second.get() << endl;
            }
            bar_rpc_handle.ordered_send<RPC_NAME(append)>("Write from 2...");
            cout << "Printing log from Bar" << endl;
            derecho::rpc::QueryResults<std::string> bar_results = bar_rpc_handle.ordered_send<RPC_NAME(print)>();
            for(auto& reply_pair : bar_results.get()) {
                cout << "Node " << reply_pair.first << " says the log is: " << reply_pair.second.get() << endl;
            }
            cout << "Clearing Bar's log" << endl;
            derecho::rpc::QueryResults<void> void_future = bar_rpc_handle.ordered_send<RPC_NAME(clear)>();
        }
    } else if(my_cache_shard != -1) {
        std::vector<node_id_t> cache_members = group.get_subgroup_members<Cache>()[my_cache_shard];
        uint32_t rank_in_cache = derecho::index_of(cache_members, my_id);
        Replicated<Cache>& cache_rpc_handle = group.get_subgroup<Cache>();
        if(rank_in_cache == 0) {
            cout << "Waiting for a 'Ken' value to appear in the cache..." << endl;
            bool found = false;
            while(!found) {
                derecho::rpc::QueryResults<bool> results = cache_rpc_handle.ordered_send<RPC_NAME(contains)>("Ken");
                derecho::rpc::QueryResults<bool>::ReplyMap& replies = results.get();
                //Fold "&&" over the results to see if they're all true
                bool contains_accum = true;
                for(auto& reply_pair : replies) {
                    bool contains_result = reply_pair.second.get();
                    cout << std::boolalpha << "  Reply from node " << reply_pair.first << ": " << contains_result << endl;
                    contains_accum = contains_accum && contains_result;
                }
                found = contains_accum;
            }
            cout << "..found!" << endl;
            derecho::rpc::QueryResults<std::string> results = cache_rpc_handle.ordered_send<RPC_NAME(get)>("Ken");
            for(auto& reply_pair : results.get()) {
                cout << "Node " << reply_pair.first << " had Ken = " << reply_pair.second.get() << endl;
            }
        } else if(rank_in_cache == 1) {
            cout << "Putting Ken = Birman in the cache" << endl;
            //Do it twice just to send more messages, so that the "contains" and "get" calls can go through
            cache_rpc_handle.ordered_send<RPC_NAME(put)>("Ken", "Birman");
            cache_rpc_handle.ordered_send<RPC_NAME(put)>("Ken", "Birman");
            //Send to node rank 2 in shard 0 of the Foo subgroup
            node_id_t p2p_target = group.get_subgroup_members<Foo>()[0][2];
            cout << "Reading Foo's state from node " << p2p_target << endl;
            PeerCaller<Foo>& p2p_foo_handle = group.get_nonmember_subgroup<Foo>();
            derecho::rpc::QueryResults<int> foo_results = p2p_foo_handle.p2p_send<RPC_NAME(read_state)>(p2p_target);
            int response = foo_results.get().get(p2p_target);
            cout << "  Response: " << response << endl;
        } else if(rank_in_cache == 2) {
            cout << "Putting Ken = Woodberry in the cache" << endl;
            cache_rpc_handle.ordered_send<RPC_NAME(put)>("Ken", "Woodberry");
            cache_rpc_handle.ordered_send<RPC_NAME(put)>("Ken", "Woodberry");
        }
    } else {
        std::cout << "This node was not assigned to any subgroup!" << std::endl;
    }

    cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
    while(true) {
    }
}
