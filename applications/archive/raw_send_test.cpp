#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <time.h>
#include <vector>

#include "block_size.h"
#include "derecho/derecho.h"
#include "rdmc/util.h"
#include "conf/conf.hpp"

using std::cout;
using std::endl;
using std::map;
using std::string;
using std::vector;

using derecho::RawObject;

int main(int argc, char *argv[]) {
    srand(time(NULL));

    uint32_t server_rank = 0;
    uint32_t node_rank;
    uint32_t num_nodes;

    map<uint32_t, std::string> node_addresses;

    rdmc::query_addresses(node_addresses, node_rank);
    num_nodes = node_addresses.size();

    vector<uint32_t> members(num_nodes);
    for(uint32_t i = 0; i < num_nodes; ++i) {
        members[i] = i;
    }

    long long unsigned int max_msg_size = 100;
    long long unsigned int block_size = get_block_size(max_msg_size);
    int num_messages = 10;
    const long long unsigned int sst_max_msg_size = (max_msg_size < 17000 ? max_msg_size : 0);
    uint num_messages_received = 0;

    auto stability_callback = [&num_messages_received](
                                      uint32_t subgroup, int sender_id, std::optional<std::pair<char*, long long int>> data,
                                      persistent::version_t ver) mutable {
        char* buf;
        long long int size;
        std::tie(buf,size) = data.value();
        cout << "Here" << endl;
        cout << buf << endl;
        num_messages_received++;
    };

    derecho::CallbackSet callbacks{stability_callback, nullptr};
    derecho::DerechoParams parameters{max_msg_size, sst_max_msg_size, block_size};

    derecho::SubgroupInfo one_raw_group{&derecho::one_subgroup_entire_view};

    std::unique_ptr<derecho::Group<RawObject>> managed_group;

    managed_group = std::make_unique<derecho::Group<RawObject>>(
            callbacks, one_raw_group,
            std::vector<derecho::view_upcall_t>{},
            &derecho::raw_object_factory);

    cout << "Finished constructing/joining ManagedGroup" << endl;

    while(managed_group->get_members().size() < num_nodes) {
    }
    auto members_order = managed_group->get_members();
    cout << "The order of members is :" << endl;
    for(auto id : members_order) {
        cout << id << " ";
    }
    cout << endl;
    derecho::Replicated<RawObject> &group_as_subgroup = managed_group->get_subgroup<RawObject>();
    for(int i = 0; i < num_messages; ++i) {
        char *buf = group_as_subgroup.get_sendbuffer_ptr(max_msg_size);
        while(!buf) {
            buf = group_as_subgroup.get_sendbuffer_ptr(max_msg_size);
        }
        for(uint i = 0; i < max_msg_size; ++i) {
            buf[i] = 'a' + (rand() % 26);
        }
        cout << buf << endl;
        group_as_subgroup.send();
    }

    while(num_messages_received < num_messages * num_nodes) {
    }
    cout << "Done" << endl;
}
