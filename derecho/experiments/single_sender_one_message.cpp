/*
 * a single sender sends a single random message to the group
 * the message is verified at the receivers
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

    std::map<uint32_t, std::string> node_address_map = initialize(node_rank, num_nodes);

    vector<uint32_t> members(num_nodes);
    for(int i = 0; i < (int)num_nodes; ++i) {
        members[i] = i;
    }

    long long unsigned int max_msg_size = 100;
    long long unsigned int block_size = 10;

    auto stability_callback = [](int sender_id, long long int index, char* buf,
                                 long long int msg_size) {
        cout << "Delivered a message" << endl;
        cout << "The message is:" << endl;
        for(int i = 0; i < msg_size; ++i) {
            cout << buf[i];
        }
        cout << endl;
    };

    std::shared_ptr<sst::SST<DerechoRow<MAX_GROUP_SIZE>, sst::Mode::Writes>>
        derecho_sst =
            std::make_shared<sst::SST<DerechoRow<8>, sst::Mode::Writes>>(
                    members, node_rank);
    vector<derecho::MessageBuffer> free_message_buffers;
    DerechoGroup<MAX_GROUP_SIZE, Dispatcher<>> g(
            members, node_rank, derecho_sst, free_message_buffers,
            Dispatcher<>(node_rank), derecho::CallbackSet{stability_callback, nullptr},
            derecho::DerechoParams{max_msg_size, block_size}, node_address_map);

    cout << "Derecho group created" << endl;

    if(node_rank == 0) {
        unsigned int msg_size = 10;
        char* buf = g.get_position(msg_size);
        for(unsigned int i = 0; i < msg_size; ++i) {
            buf[i] = rand() % 26 + 'a';
        }
        cout << "Calling send" << endl;
        g.send();
        cout << "send call finished" << endl;
    }
    while(true) {
    }

    return 0;
}
