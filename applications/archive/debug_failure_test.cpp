/*
 * This test debugs a failure case where the group is provisioned for both 2 and 3 members. When there are 3 members, if the last member fails, then we get a map::at exception because of the SST containing garbage num_received entries. If the middle member fails, then nothing happens (but the SST still has garbage entries).
 */

#include <iostream>

#include "derecho/derecho.h"

using std::cout;
using std::endl;

using namespace derecho;

int main(int argc, char *argv[]) {
    pthread_setname_np(pthread_self(), "failure_test");

    Conf::initialize(argc, argv);

    auto membership_function = [](const std::vector<std::type_index>& subgroup_type_order,
            const std::unique_ptr<derecho::View>& prev_view, derecho::View& curr_view) {
        subgroup_shard_layout_t subgroup_vector(1);
        subgroup_vector[0].emplace_back(curr_view.make_subview(curr_view.members));
        curr_view.next_unassigned_rank = curr_view.members.size();
        derecho::subgroup_allocation_map_t subgroup_allocation;
        subgroup_allocation.emplace(std::type_index(typeid(RawObject)), std::move(subgroup_vector));
        return subgroup_allocation;
    };

    SubgroupInfo one_raw_group(membership_function);

    Group<RawObject> managed_group(
            {}, one_raw_group, {},
	    {}, &raw_object_factory);

    cout << "Finished constructing/joining ManagedGroup" << endl;

    auto members = managed_group.get_members();
    cout << "The order of members is :" << endl;
    for(auto m : members) {
        cout << m << " ";
    }
    cout << endl;

    while(true) {
      
    }
}
