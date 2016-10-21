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
using derecho::DerechoRow;

constexpr int MAX_GROUP_SIZE = 8;

int main() {
    srand(time(NULL));

    uint32_t node_rank;
    uint32_t num_nodes;

    initialize(node_rank, num_nodes);

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

    std::shared_ptr<sst::SST<DerechoRow<MAX_GROUP_SIZE>, sst::Mode::Writes>>
        derecho_sst = std::make_shared<
            sst::SST<DerechoRow<MAX_GROUP_SIZE>, sst::Mode::Writes>>(members,
                                                                     node_rank);
    vector<derecho::MessageBuffer> free_message_buffers;
    DerechoGroup<MAX_GROUP_SIZE> g(
        members, node_rank, derecho_sst, free_message_buffers, max_msg_size,
        derecho::CallbackSet{stability_callback, nullptr}, block_size);

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
