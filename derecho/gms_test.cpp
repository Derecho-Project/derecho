#include <iostream>
#include <string>
#include <cstdlib>
#include <map>

using std::string;
using std::cin;
using std::cout;
using std::endl;
using std::map;

#include "derecho_group.h"
#include "experiments/block_size.h"
#include "rdmc/util.h"
#include "managed_group.h"
#include "view.h"

static const int GMS_PORT = 12345;

int main(int argc, char *argv[]) {
    try {
        if(argc < 2) {
            cout << "Error: Expected leader's node ID as the first argument."
                 << endl;
            return -1;
        }
        uint32_t server_rank = std::atoi(argv[1]);
        uint32_t num_nodes;
        uint32_t node_rank;

        map<uint32_t, std::string> node_addresses;

        query_addresses(node_addresses, node_rank);
        num_nodes = node_addresses.size();
        long long unsigned int max_msg_size = 1000000;
        long long unsigned int block_size = 100000;

        int num_messages = 100000;

        bool done = false;
        auto stability_callback = [&num_messages, &done, &num_nodes](
            int sender_rank, long long int index, char *buf,
            long long int msg_size) {
            cout << "In stability callback; sender = " << sender_rank
                 << ", index = " << index << endl;
            if(index == num_messages - 1 && sender_rank == (int)num_nodes - 1) {
                done = true;
            }
        };

        derecho::ManagedGroup managed_group(
            GMS_PORT, node_addresses, node_rank, server_rank, max_msg_size,
            derecho::CallbackSet{stability_callback, nullptr}, block_size);

        cout << "Finished constructing/joining ManagedGroup" << endl;

        while(managed_group.get_members().size() < num_nodes) {
        }

        for(int i = 0; i < num_messages; ++i) {
            // random message size between 1 and 100
            unsigned int msg_size = (rand() % 7 + 2) * (max_msg_size / 10);
            char *buf = managed_group.get_sendbuffer_ptr(msg_size);
            //        cout << "After getting sendbuffer for message " << i <<
            //        endl;
            //        managed_group.debug_print_status();
            while(!buf) {
                buf = managed_group.get_sendbuffer_ptr(msg_size);
            }
            for(unsigned int j = 0; j < msg_size; ++j) {
                buf[j] = 'a' + i;
            }
            //        cout << "Client telling DerechoGroup to send message " <<
            //        i << "
            //        with size " << msg_size << endl;;
            managed_group.send();
        }
        while(!done) {
        }

        managed_group.barrier_sync();

        managed_group.leave();

    } catch(const std::exception &e) {
        cout << "Main got an exception: " << e.what() << endl;
        throw e;
    }

    cout << "Finished destroying managed_group" << endl;
}
