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

    auto stability_callback = [](uint32_t subgroup, int sender_id, long long int index, std::optional<std::pair<char*, long long int>> data, persistent::version_t ver) mutable {
        char* buf;
        long long int msg_size;
        std::tie(buf, msg_size) = data.value();
        cout << "=== Delivered a message from sender with id " << sender_id << endl;
        cout << "Message contents: " << endl;
        for(auto i = 0; i < msg_size; ++i) {
            cout << buf[i];
        }
        cout << endl << endl;
    };

    auto membership_function = [num_nodes](const std::type_index& subgroup_type,
            const std::unique_ptr<derecho::View>& prev_view, derecho::View& curr_view) {
        subgroup_shard_layout_t subgroup_vector(1);
        auto num_members = curr_view.members.size();
        if(num_members < num_nodes) {
            throw subgroup_provisioning_exception();
        }
        subgroup_vector[0].emplace_back(curr_view.make_subview(curr_view.members));
        curr_view.next_unassigned_rank = curr_view.members.size();
        return subgroup_vector;
    };

    SubgroupInfo one_raw_group(membership_function);

    Group<RawObject> managed_group(
            CallbackSet{stability_callback},
            one_raw_group, nullptr, 
            std::vector<view_upcall_t>{},
            &raw_object_factory);

    cout << "Finished constructing/joining ManagedGroup" << endl;

    auto members_order = managed_group.get_members();
    cout << "The order of members is :" << endl;
    for(uint i = 0; i < num_nodes; ++i) {
        cout << members_order[i] << " ";
    }
    cout << endl;

    Replicated<RawObject> &group_as_subgroup = managed_group.get_subgroup<RawObject>();
    while(true) {
        std::string msg_str;
        std::getline(std::cin, msg_str);
        if(!msg_str.size()) {
            continue;
        }
        group_as_subgroup.send(msg_str.size(), [&](char* buf) {
            msg_str.copy(buf, msg_str.size());
        });
    }
}
