/*
 * multiple senders send multiple random messages of random size to the group
 */

#include <iostream>
#include <time.h>
#include <vector>

#include "derecho/derecho.h"
#include "initialize.h"

using derecho::RawObject;
using std::cin;
using std::cout;
using std::endl;
using std::vector;

constexpr int MAX_GROUP_SIZE = 8;

int main() {
    srand(time(NULL));

    derecho::node_id_t node_id;
    derecho::ip_addr my_ip;
    derecho::ip_addr leader_ip;

    query_node_info(node_id, my_ip, leader_ip);

    long long unsigned int max_msg_size = 100;
    long long unsigned int block_size = 10;
    const long long unsigned int sst_max_msg_size = (max_msg_size < 17000 ? max_msg_size : 0);

    int num_messages = 1000;

    auto stability_callback = [](uint32_t subgroup, int sender_id, long long int index,
                                 std::optional<std::pair<uint8_t*, long long int>> data, persistent::version_t ver) {
        cout << "Sender: " << sender_id << ", index: " << index << endl;
    };

    derecho::SubgroupInfo one_raw_group{{{std::type_index(typeid(RawObject)), &derecho::one_subgroup_entire_view}},
                                        {std::type_index(typeid(RawObject))}};

    std::unique_ptr<derecho::Group<>> g;
    if(node_id == 0) {
        g = std::make_unique<derecho::Group<>>(node_id, my_ip,
                                               derecho::UserMessageCallbacks{stability_callback, nullptr},
                                               one_raw_group,
                                               derecho::DerechoParams{max_msg_size, sst_max_msg_size, block_size});
    } else {
        g = std::make_unique<derecho::Group<>>(node_id, my_ip, leader_ip,
                                               derecho::UserMessageCallbacks{stability_callback, nullptr},
                                               one_raw_group);
    }
    for(int i = 0; i < num_messages; ++i) {
        derecho::RawSubgroup &sg = g->get_subgroup<RawObject>();
        // random message size between 1 and 100
        unsigned int msg_size = (rand() % 7 + 2) * 10;
        uint8_t *buf = sg.get_sendbuffer_ptr(msg_size);
        while(!buf) {
            buf = sg.get_sendbuffer_ptr(msg_size);
        }
        for(unsigned int j = 0; j < msg_size; ++j) {
            buf[j] = 'a' + i;
        }
        sg.send();
    }
    while(true) {
    }

    return 0;
}
