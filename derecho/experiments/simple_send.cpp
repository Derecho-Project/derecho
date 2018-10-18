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
    srand(time(NULL));

    uint32_t server_rank = 0;
    uint32_t node_id;
    uint32_t num_nodes;

    map<uint32_t, std::string> node_addresses;

    rdmc::query_addresses(node_addresses, node_id);
    num_nodes = node_addresses.size();

    vector<uint32_t> members(num_nodes);
    for(uint32_t i = 0; i < num_nodes; ++i) {
        members[i] = i;
    }

    const long long unsigned int max_msg_size = 100;
    const long long unsigned int block_size = 100;
    const long long unsigned int sst_max_msg_size = (max_msg_size < 17000 ? max_msg_size : 0);

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

    auto membership_function = [num_nodes](const View &curr_view, int &next_unassigned_rank) {
        subgroup_shard_layout_t subgroup_vector(1);
        auto num_members = curr_view.members.size();
        if(num_members < num_nodes) {
            throw derecho::subgroup_provisioning_exception();
        }
        subgroup_vector[0].emplace_back(curr_view.make_subview(curr_view.members));
        next_unassigned_rank = curr_view.members.size();
        return subgroup_vector;
    };

    std::map<std::type_index, shard_view_generator_t> subgroup_map = {{std::type_index(typeid(RawObject)), membership_function}};
    derecho::SubgroupInfo one_raw_group(subgroup_map);

    std::unique_ptr<derecho::Group<>> managed_group;
    if(node_id == server_rank) {
        managed_group = std::make_unique<derecho::Group<>>(
                node_id, node_addresses[node_id],
                derecho::CallbackSet{stability_callback, nullptr},
                one_raw_group,
                derecho::DerechoParams{max_msg_size, sst_max_msg_size, block_size});
    } else {
        managed_group = std::make_unique<derecho::Group<>>(
                node_id, node_addresses[node_id],
                node_addresses[server_rank],
                derecho::CallbackSet{stability_callback, nullptr},
                one_raw_group);
    }

    cout << "Finished constructing/joining ManagedGroup" << endl;

    while(managed_group->get_members().size() < num_nodes) {
    }
    uint32_t node_rank = -1;
    auto members_order = managed_group->get_members();
    cout << "The order of members is :" << endl;
    for(uint i = 0; i < num_nodes; ++i) {
        cout << members_order[i] << " ";
        if(members_order[i] == node_id) {
            node_rank = i;
        }
    }
    cout << endl;

    RawSubgroup &group_as_subgroup = managed_group->get_subgroup<RawObject>();
    std::cin.sync();
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
