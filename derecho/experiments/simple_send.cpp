#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <time.h>
#include <vector>

#include "derecho/derecho.h"
#include "rdmc/rdmc.h"
#include "rdmc/util.h"

using std::cout;
using std::endl;
using std::map;
using std::vector;

using namespace derecho;

int main(int argc, char *argv[]) {
    pthread_setname_np(pthread_self(), "simple_send");

    if(argc < 2) {
      std::cout << "Usage: " << argv[0] << " <num_nodes> [configuration options...]" << std::endl;
      return -1;
    }

    const uint32_t num_nodes = std::stoi(argv[1]);

    Conf::initialize(argc, argv);

    auto stability_callback = [](uint32_t subgroup, int sender_id, long long int index, char *buf, long long int msg_size) mutable {
        // null message filter
        if(msg_size == 0) {
            cout << "Received a null message from sender with id " << sender_id << endl;
            return;
        }

        cout << "=== Delivered a message from sender with id " << sender_id << endl;
        cout << "Message contents: " << endl;
        for(auto i = 0; i < msg_size; ++i) {
            cout << buf[i];
        }
        cout << endl << endl;
    };

    auto membership_function = [num_nodes](const View &curr_view, int &next_unassigned_rank, bool previous_was_successful) {
        subgroup_shard_layout_t subgroup_vector(1);
        auto num_members = curr_view.members.size();
        if(num_members < num_nodes) {
            throw subgroup_provisioning_exception();
        }
        subgroup_vector[0].emplace_back(curr_view.make_subview(curr_view.members));
        next_unassigned_rank = curr_view.members.size();
        return subgroup_vector;
    };

    std::map<std::type_index, shard_view_generator_t> subgroup_map = {{std::type_index(typeid(RawObject)), membership_function}};
    SubgroupInfo one_raw_group(subgroup_map);

    Group<> managed_group = Group<>(
            CallbackSet{stability_callback, nullptr},
            one_raw_group);

    cout << "Finished constructing/joining ManagedGroup" << endl;

    while(managed_group.get_members().size() < num_nodes) {
    }
    auto members_order = managed_group.get_members();
    cout << "The order of members is :" << endl;
    for(uint i = 0; i < num_nodes; ++i) {
        cout << members_order[i] << " ";
    }
    cout << endl;

    RawSubgroup &group_as_subgroup = managed_group.get_subgroup<RawObject>();
    while(true) {
	std::string msg_str;
	std::getline(std::cin, msg_str);
	if (!msg_str.size()) {
	  continue;
	}
        char *buf = group_as_subgroup.get_sendbuffer_ptr(msg_str.size());
        while(!buf) {
	  buf = group_as_subgroup.get_sendbuffer_ptr(msg_str.size());
        }
	msg_str.copy(buf, msg_str.size());
	group_as_subgroup.send();
    }
}
