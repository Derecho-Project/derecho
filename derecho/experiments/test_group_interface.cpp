#include <vector>

#include "../derecho_group.h"
#include "initialize.h"

using namespace sst;
using std::cout;
using std::endl;
using std::cin;
using std::vector;
using derecho::DerechoGroup;
using derecho::DerechoRow;

constexpr int MAX_GROUP_SIZE = 8;

int main() {
    uint32_t node_rank;
    uint32_t num_nodes;

    auto node_addresses = initialize(node_rank, num_nodes);

    vector<uint32_t> members(num_nodes);
    for(int i = 0; i < (int)num_nodes; ++i) {
        members[i] = i;
    }

    long long unsigned int max_msg_size = 100;
    long long unsigned int block_size = 10;

    auto stability_callback = [](int sender_id, long long int index, char* buf,
                                 long long int msg_size) {
        cout << "Some message is stable" << endl;
    };
    derecho::CallbackSet callbacks{stability_callback,
                                   derecho::message_callback{}};

    std::shared_ptr<sst::SST<DerechoRow<MAX_GROUP_SIZE>, sst::Mode::Writes>>
        derecho_sst =
            std::make_shared<sst::SST<DerechoRow<8>, sst::Mode::Writes>>(
                members, node_rank);
    vector<derecho::MessageBuffer> free_message_buffers;
    derecho::DerechoParams parameters{max_msg_size, block_size};

    DerechoGroup<MAX_GROUP_SIZE, Dispatcher<>> g(members, node_rank, derecho_sst,
                                   free_message_buffers, Dispatcher<>(node_rank),
                                   callbacks, parameters, node_addresses);

    cout << "Derecho group created" << endl;

    if(node_rank == 0) {
        int msg_size = 50;
        char* buf = g.get_position(msg_size);
        for(int i = 0; i < msg_size; ++i) {
            buf[i] = 'a';
        }
        cout << "Calling send" << endl;
        g.send();
        cout << "send call finished" << endl;
    }
    while(true) {
        int n;
        cin >> n;
        g.debug_print();
    }

    return 0;
}
