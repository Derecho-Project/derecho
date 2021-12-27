/**
 * @file repeated_rpc_test.cpp
 *
 * @date Oct 11, 2017
 * @author edward
 */

#include <iostream>

#include <derecho/core/derecho.hpp>
#include "test_objects.h"
#include <derecho/conf/conf.hpp>

using derecho::Replicated;
using std::cout;
using std::endl;

int main(int argc, char** argv) {
    derecho::Conf::initialize(argc, argv);

    derecho::UserMessageCallbacks callback_set{};

    const int num_nodes_in_test = 3;
    derecho::SubgroupInfo subgroup_info{[](const std::type_index& subgroup_type,
            const std::unique_ptr<derecho::View>& prev_view, derecho::View& curr_view,
            derecho::subgroup_allocation_map_t& subgroup_allocation) {
        if(subgroup_allocation.at(subgroup_type)) {
            return;
        }
        if(curr_view.num_members < num_nodes_in_test) {
            throw derecho::subgroup_provisioning_exception();
        }
        auto subgroup_vector = std::make_unique<derecho::subgroup_shard_layout_t>(1);
        std::vector<node_id_t> node_list(&curr_view.members[0], &curr_view.members[0] + num_nodes_in_test);
        //Put the desired SubView at subgroup_vector[0][0] since there's one subgroup with one shard
        (*subgroup_vector)[0].emplace_back(curr_view.make_subview(node_list));
        curr_view.next_unassigned_rank = std::max(curr_view.next_unassigned_rank, num_nodes_in_test);
        cout << "Foo function setting next_unassigned_rank to " << curr_view.next_unassigned_rank << endl;
        subgroup_allocation[subgroup_type] = std::move(subgroup_vector);
    }};

    auto foo_factory = [](PersistentRegistry*) { return std::make_unique<Foo>(-1); };

    derecho::Group<Foo> group(callback_set, subgroup_info, std::vector<derecho::view_upcall_t>{}, foo_factory);

    cout << "Finished constructing/joining Group" << endl;

    Replicated<Foo>& foo_rpc_handle = group->get_subgroup<Foo>();
    int trials = 1000;
    cout << "Changing Foo's state " << trials << " times" << endl;
    for(int count = 0; count < trials; ++count) {
        cout << "Sending query #" << count << std::endl;
        derecho::rpc::QueryResults<bool> results = foo_rpc_handle.ordered_send<RPC_NAME(change_state)>(count);
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
