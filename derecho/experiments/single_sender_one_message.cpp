/*
 * a single sender sends a single random message to the group
 * the message is verified at the receivers
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
using derecho::MulticastGroup;
using derecho::DerechoSST;

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
        cout << "Delivered a message" << endl;
        cout << "The message is:" << endl;
        for(int i = 0; i < msg_size; ++i) {
            cout << buf[i];
        }
        cout << endl;
    };

    using derecho::RawObject;
    derecho::SubgroupInfo one_raw_group{{{std::type_index(typeid(RawObject)), &derecho::one_subgroup_entire_view}}};

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

    cout << "Derecho group created" << endl;

    if(node_id == 0) {
        derecho::RawSubgroup& sg = g->get_subgroup<RawObject>();
        unsigned int msg_size = 10;
        char* buf = sg.get_sendbuffer_ptr(msg_size);
        for(unsigned int i = 0; i < msg_size; ++i) {
            buf[i] = rand() % 26 + 'a';
        }
        cout << "Calling send" << endl;
        sg.send();
        cout << "send call finished" << endl;
    }
    while(true) {
    }

    return 0;
}
