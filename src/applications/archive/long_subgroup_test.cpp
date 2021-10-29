#include <iostream>
#include <sstream>

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

    derecho::SubgroupInfo subgroup_info{[](const std::vector<std::type_index>& subgroup_type_order,
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
    //for the subgroup's initial state
    auto foo_factory = [](PersistentRegistry*,derecho::subgroup_id_t) { return std::make_unique<Foo>(-1); };
    auto bar_factory = [](PersistentRegistry*,derecho::subgroup_id_t) { return std::make_unique<Bar>(); };
    auto cache_factory = [](PersistentRegistry*,derecho::subgroup_id_t) { return std::make_unique<Cache>(); };

    derecho::Group<Foo, Bar, Cache> group({}, subgroup_info, {},
                                           std::vector<derecho::view_upcall_t>{},
                                           foo_factory, bar_factory, cache_factory);

    cout << "Finished constructing/joining Group" << endl;

    const uint32_t my_id = derecho::getConfUInt32(CONF_DERECHO_LOCAL_ID);
    std::vector<node_id_t> foo_members = group.get_subgroup_members<Foo>(0)[0];
    std::vector<node_id_t> cache_members = group.get_subgroup_members<Cache>(0)[0];
    auto find_in_foo_results = std::find(foo_members.begin(), foo_members.end(), my_id);
    if(find_in_foo_results != foo_members.end()) {
        Replicated<Foo>& foo_rpc_handle = group.get_subgroup<Foo>();
        Replicated<Bar>& bar_rpc_handle = group.get_subgroup<Bar>();
        int trials = 1000;
        cout << "Changing Foo's state " << trials << " times" << endl;
        for(int count = 0; count < trials; ++count) {
            derecho::rpc::QueryResults<bool> results = foo_rpc_handle.ordered_send<RPC_NAME(change_state)>(count);
            bool results_total = true;
            for(auto& reply_pair : results.get()) {
                results_total = results_total && reply_pair.second.get();
            }
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
        cout << "Changing Bar's state " << trials << " times" << endl;
        for(int count = 0; count < trials; ++count) {
            std::stringstream string_builder;
            string_builder << "Node " << my_id << " Update " << count << "  ";
            bar_rpc_handle.ordered_send<RPC_NAME(append)>(string_builder.str());
        }
        PeerCaller<Cache>& cache_p2p_handle = group.get_nonmember_subgroup<Cache>();
        int p2p_target = 4;
        derecho::rpc::QueryResults<std::string> result = cache_p2p_handle.p2p_send<RPC_NAME(get)>(p2p_target, "Stuff");
        std::string response = result.get().get(p2p_target);
        cout << "Node " << p2p_target << " had cache entry Stuff = " << response << endl;
    } else {
        Replicated<Cache>& cache_rpc_handle = group.get_subgroup<Cache>();
        int trials = 1000;
        if(my_id == 7) {
            trials -= 100;
        }
        cout << "Changing Cache's state " << trials << " times" << endl;
        for(int count = 0; count < trials; ++count) {
            std::stringstream string_builder;
            string_builder << "Node " << my_id << " update " << count;
            cache_rpc_handle.ordered_send<RPC_NAME(put)>("Stuff", string_builder.str());
            if(my_id == 5 && count == 100) {
                //I want to test this node crashing and re-joining with a different ID
                std::this_thread::sleep_for(std::chrono::seconds(1));
                return 0;
            }
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
        PeerCaller<Foo>& foo_p2p_handle = group.get_nonmember_subgroup<Foo>();
        node_id_t foo_p2p_target = foo_members[1];
        derecho::rpc::QueryResults<int> foo_result = foo_p2p_handle.p2p_send<RPC_NAME(read_state)>(foo_p2p_target);
        cout << "Node " << foo_p2p_target << " returned Foo state = " << foo_result.get().get(foo_p2p_target) << endl;
        PeerCaller<Bar>& bar_p2p_handle = group.get_nonmember_subgroup<Bar>();
        node_id_t bar_p2p_target = foo_members[0];
        derecho::rpc::QueryResults<std::string> bar_result = bar_p2p_handle.p2p_send<RPC_NAME(print)>(bar_p2p_target);
    }

    cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
    while(true) {
    }
}
