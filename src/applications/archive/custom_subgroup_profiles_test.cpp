#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <derecho/core/derecho.hpp>
#include "test_objects.hpp"
#include <derecho/conf/conf.hpp>

using derecho::PeerCaller;
using derecho::Replicated;
using std::cout;
using std::endl;
using namespace persistent;

int main(int argc, char** argv) {
    derecho::Conf::initialize(argc, argv);

    //Since this is just a test, assume there will always be 6 members with IDs 0-5
    //Assign Foo and Bar to a subgroup containing 0, 1, and 2, and Cache to a subgroup containing 3, 4, and 5
    derecho::SubgroupInfo subgroup_info{[](const std::vector<std::type_index>& subgroup_type_order,
                                           const std::unique_ptr<derecho::View>& prev_view, derecho::View& curr_view) {
        derecho::subgroup_allocation_map_t subgroup_allocation;
        for(const auto& subgroup_type : subgroup_type_order) {
            derecho::subgroup_shard_layout_t subgroup_layout(1);
            if(subgroup_type == std::type_index(typeid(Foo)) || subgroup_type == std::type_index(typeid(Bar))) {
                if(curr_view.num_members < 3) {
                    throw derecho::subgroup_provisioning_exception();
                }
                std::vector<node_id_t> first_3_nodes(&curr_view.members[0], &curr_view.members[0] + 3);
                //Put the desired SubView at subgroup_layout[0][0] since there's one subgroup with one shard
                subgroup_layout[0].emplace_back(curr_view.make_subview(first_3_nodes, derecho::Mode::ORDERED, {}, "SMALL"));
                curr_view.next_unassigned_rank = std::max(curr_view.next_unassigned_rank, 3);
            } else { /* subgroup_type == std::type_index(typeid(Cache)) */
                if(curr_view.num_members < 6) {
                    throw derecho::subgroup_provisioning_exception();
                }
                std::vector<node_id_t> next_3_nodes(&curr_view.members[3], &curr_view.members[3] + 3);
                subgroup_layout[0].emplace_back(curr_view.make_subview(next_3_nodes, derecho::Mode::ORDERED, {}, "LARGE"));
                curr_view.next_unassigned_rank += 3;
            }
            subgroup_allocation.emplace(subgroup_type, std::move(subgroup_layout));
        }
        return subgroup_allocation;
    }};


    //Each replicated type needs a factory; this can be used to supply constructor arguments
    //for the subgroup's initial state
    auto foo_factory = [](PersistentRegistry*,derecho::subgroup_id_t) { return std::make_unique<Foo>(-1); };
    auto bar_factory = [](PersistentRegistry*,derecho::subgroup_id_t) { return std::make_unique<Bar>(); };
    auto cache_factory = [](PersistentRegistry*,derecho::subgroup_id_t) { return std::make_unique<Cache>(); };

    derecho::Group<Foo, Bar, Cache> group({}, subgroup_info, {},
                                          std::vector<derecho::view_upcall_t>{},
                                          foo_factory, bar_factory, cache_factory);
    cout << "Finished constructing/joining Group" << endl;

    const uint32_t node_rank = group.get_my_rank();
    if(node_rank == 0) {
        Replicated<Foo>& foo_rpc_handle = group.get_subgroup<Foo>();
        Replicated<Bar>& bar_rpc_handle = group.get_subgroup<Bar>();
        cout << "Appending to Bar" << endl;
        bar_rpc_handle.ordered_send<RPC_NAME(append)>("Write from 0...");
        cout << "Reading Foo's state just to allow node 1's message to be delivered" << endl;
        foo_rpc_handle.ordered_send<RPC_NAME(read_state)>();
    }
    if(node_rank == 1) {
        Replicated<Foo>& foo_rpc_handle = group.get_subgroup<Foo>();
        Replicated<Bar>& bar_rpc_handle = group.get_subgroup<Bar>();
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
    }
    if(node_rank == 2) {
        Replicated<Foo>& foo_rpc_handle = group.get_subgroup<Foo>();
        Replicated<Bar>& bar_rpc_handle = group.get_subgroup<Bar>();
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
        bar_rpc_handle.ordered_send<RPC_NAME(clear)>();
    }

    if(node_rank == 3) {
        Replicated<Cache>& cache_rpc_handle = group.get_subgroup<Cache>();
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
    }
    if(node_rank == 4) {
        Replicated<Cache>& cache_rpc_handle = group.get_subgroup<Cache>();
        cout << "Putting Ken = Birman in the cache" << endl;
        //Do it twice just to send more messages, so that the "contains" and "get" calls can go through
        cache_rpc_handle.ordered_send<RPC_NAME(put)>("Ken", "Birman");
        // this ASSUMES that node id 2 is part of this subgroup
        // this will be true if rank is equal to id
        node_id_t p2p_target = 2;
        cout << "Reading Foo's state from node " << p2p_target << endl;
        PeerCaller<Foo>& p2p_foo_handle = group.get_nonmember_subgroup<Foo>();
        derecho::rpc::QueryResults<int> foo_results = p2p_foo_handle.p2p_send<RPC_NAME(read_state)>(p2p_target);
        int response = foo_results.get().get(p2p_target);
        cout << "  Response: " << response << endl;
    }
    if(node_rank == 5) {
        Replicated<Cache>& cache_rpc_handle = group.get_subgroup<Cache>();
        cout << "Putting Ken = Woodberry in the cache" << endl;
        cache_rpc_handle.ordered_send<RPC_NAME(put)>("Ken", "Woodberry");
    }

    cout << "Reached end of main(), entering infinite loop so program doesn't exit" << endl;
    while(true) {
    }
}
