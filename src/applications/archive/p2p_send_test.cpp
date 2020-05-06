#include <iostream>
#include <sstream>

#include <derecho/conf/conf.hpp>
#include <derecho/core/derecho.hpp>
#include "test_objects.hpp"

using derecho::ExternalCaller;
using derecho::Replicated;
using std::cout;
using std::endl;
using namespace persistent;

int main(int argc, char** argv) {
    derecho::Conf::initialize(argc, argv);
    derecho::SubgroupInfo subgroup_info{[](const std::vector<std::type_index>& subgroup_type_order,
            const std::unique_ptr<derecho::View>& prev_view, derecho::View& curr_view) {
        if(curr_view.num_members < 3) {
            throw derecho::subgroup_provisioning_exception();
        }
        derecho::subgroup_shard_layout_t subgroup_layout(1);
        std::vector<node_id_t> first_3_nodes(&curr_view.members[0], &curr_view.members[0] + 3);
        //Put the desired SubView at subgroup_layout[0][0] since there's one subgroup with one shard
        subgroup_layout[0].emplace_back(curr_view.make_subview(first_3_nodes));
        curr_view.next_unassigned_rank = std::max(curr_view.next_unassigned_rank, 3);
        //Since we know there is only one subgroup type, just put a single entry in the map
        derecho::subgroup_allocation_map_t subgroup_allocation;
        subgroup_allocation.emplace(std::type_index(typeid(Foo)), std::move(subgroup_layout));
        return subgroup_allocation;
    }};

    //Each replicated type needs a factory; this can be used to supply constructor arguments
    //for the subgroup's initial state
    auto foo_factory = [](PersistentRegistry*,derecho::subgroup_id_t) { return std::make_unique<Foo>(-1); };

    derecho::Group<Foo> group(
            {}, subgroup_info, nullptr,
            std::vector<derecho::view_upcall_t>{},
            foo_factory);

    cout << "Finished constructing/joining Group" << endl;

    uint32_t my_rank = group.get_my_rank();
    Replicated<Foo>& foo_rpc_handle = group.get_subgroup<Foo>();
    {
        if(my_rank <= 2) {
            foo_rpc_handle.p2p_send<RPC_NAME(change_state)>(0, 75 + my_rank * 35);
        }
    }
    std::this_thread::sleep_for(std::chrono::seconds(3));
    {
        if(my_rank <= 2) {
            auto result = foo_rpc_handle.p2p_send<RPC_NAME(read_state)>(0);
            auto response = result.get().get(0);
            cout << "Node 0 had state = " << response << endl;
        }
    }

    cout << "Reached end of main(), entering infinite loop so program doesn't exit" << std::endl;
    while(true) {
    }
}
