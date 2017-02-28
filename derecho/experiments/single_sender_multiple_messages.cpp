/*
 * a single sender sends multiple random messages of random size to the group
 */

#include <iostream>
#include <vector>
#include <time.h>

#include "derecho/derecho.h"
#include "initialize.h"

using std::cout;
using std::endl;
using std::cin;
using std::vector;
using derecho::RawObject;

constexpr int MAX_GROUP_SIZE = 8;

int main() {
    srand(time(NULL));

    derecho::node_id_t node_id;
    derecho::ip_addr my_ip;
    derecho::ip_addr leader_ip;

    query_node_info(node_id, my_ip, leader_ip);

    long long unsigned int max_msg_size = 100;
    long long unsigned int block_size = 10;

    auto stability_callback = [](uint32_t subgroup, int sender_id, long long int index, char* buf,
                                 long long int msg_size) {
        cout << "Message " << index << " by node " << sender_id << " of size "
             << msg_size << " is stable " << endl;
    };

    derecho::SubgroupInfo one_raw_group{ {{std::type_index(typeid(RawObject)), 1}},
        {{std::type_index(typeid(RawObject)), &derecho::one_subgroup_entire_view}}
    };
    std::unique_ptr<derecho::Group<>> g;
    if(node_id == 0) {
        g = std::make_unique<derecho::Group<>>(my_ip,
                derecho::CallbackSet{stability_callback, nullptr},
                one_raw_group,
                derecho::DerechoParams{max_msg_size, block_size});
    } else {
        g = std::make_unique<derecho::Group<>>(node_id, my_ip, leader_ip,
                derecho::CallbackSet{stability_callback, nullptr},
                one_raw_group);
    }

    int num_messages = 100;
    if(node_id == 0) {
        derecho::RawSubgroup& sg = g->get_subgroup<RawObject>();
        for(int i = 0; i < num_messages; ++i) {
            // random message size between 1 and 100
            int msg_size = (rand() % 7 + 2) * 10;
            char* buf = sg.get_sendbuffer_ptr(msg_size);
            while(!buf) {
                buf = sg.get_sendbuffer_ptr(msg_size);
            }
            for(int j = 0; j < msg_size; ++j) {
                buf[j] = rand() % 26 + 'a';
            }
            sg.send();
        }
    }
    while(true) {
    }

    return 0;
}
