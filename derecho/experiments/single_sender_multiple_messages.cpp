/*
 * a single sender sends multiple random messages of random size to the group
 */

#include <iostream>
#include <vector>
#include <time.h>

#include "../derecho_group.h"
#include "initialize.h"

using std::cout;
using std::endl;
using std::cin;
using std::vector;
using derecho::DerechoGroup;
using derecho::DerechoSST;

constexpr int MAX_GROUP_SIZE = 8;

int main() {
    srand(time(NULL));

    uint32_t node_rank;
    uint32_t num_nodes;

    std::map<uint32_t, std::string> node_address_map = initialize(node_rank, num_nodes);

    vector<uint32_t> members(num_nodes);
    for(int i = 0; i < (int)num_nodes; ++i) {
        members[i] = i;
    }

    long long unsigned int max_msg_size = 100;
    long long unsigned int block_size = 10;

    auto stability_callback = [](int sender_id, long long int index, char* buf,
                                 long long int msg_size) {
        cout << "Message " << index << " by node " << sender_id << " of size "
             << msg_size << " is stable " << endl;
    };

    std::shared_ptr<DerechoSST> derecho_sst =
            std::make_shared<DerechoSST>(sst::SSTParams(members, node_rank));
    vector<derecho::MessageBuffer> free_message_buffers;
    DerechoGroup<Dispatcher<>> g(
        members, node_rank, derecho_sst, free_message_buffers,
        Dispatcher<>(node_rank), derecho::CallbackSet{stability_callback, nullptr},
        derecho::DerechoParams{max_msg_size, block_size}, node_address_map);

    int num_messages = 100;
    if(node_rank == 0) {
        for(int i = 0; i < num_messages; ++i) {
            // random message size between 1 and 100
            int msg_size = (rand() % 7 + 2) * 10;
            char* buf = g.get_position(msg_size);
            while(!buf) {
                buf = g.get_position(msg_size);
            }
            for(int j = 0; j < msg_size; ++j) {
                buf[j] = rand() % 26 + 'a';
            }
            g.send();
        }
    }
    while(true) {
    }

    return 0;
}
