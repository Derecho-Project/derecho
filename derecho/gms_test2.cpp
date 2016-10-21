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
#include "derecho_caller.h"
#include "experiments/block_size.h"
#include "managed_group.h"
#include "view.h"

void query_node_info(derecho::node_id_t& node_id, derecho::ip_addr& node_ip, derecho::ip_addr& leader_ip) {
     cout << "Please enter this node's ID: ";
     cin >> node_id;
     cout << "Please enter this node's IP address: ";
     cin >> node_ip;
     cout << "Please enter the leader node's IP address: ";
     cin >> leader_ip;
}

int main(int argc, char *argv[]) {
    try {
        if(argc < 2) {
            cout << "Error: Expected number of nodes in experiment as the first argument."
                 << endl;
            return -1;
        }
        uint32_t num_nodes = std::atoi(argv[1]);
        derecho::node_id_t node_id;
        derecho::ip_addr my_ip;
        derecho::node_id_t leader_id = 0;
        derecho::ip_addr leader_ip;

        query_node_info(node_id, my_ip, leader_ip);

        long long unsigned int max_msg_size = 100;
        long long unsigned int block_size = 10;

        int num_messages = 1000;

        bool done = false;
        auto stability_callback = [&num_messages, &done, &num_nodes](
            int sender_rank, long long int index, char *buf,
            long long int msg_size) {
            cout << "In stability callback; sender rank = " << sender_rank
                 << ", index = " << index << endl;
            printf("Message: %.*s\n", (int)msg_size, buf);
            if(index == num_messages - 1 && sender_rank == (int)num_nodes - 1) {
                done = true;
            }
        };

        std::this_thread::sleep_for(std::chrono::milliseconds{10 * node_id});

        derecho::CallbackSet callbacks{stability_callback, nullptr};
        derecho::DerechoParams param_object{max_msg_size, block_size};
        Dispatcher<> empty_dispatcher(node_id);
        std::unique_ptr<derecho::ManagedGroup<Dispatcher<>>> managed_group;


         if(node_id == leader_id) {
            managed_group = std::make_unique<derecho::ManagedGroup<Dispatcher<>>>(
                    my_ip, std::move(empty_dispatcher), callbacks, param_object);
        } else {
            managed_group = std::make_unique<derecho::ManagedGroup<Dispatcher<>>>(
                    node_id, my_ip, leader_id, leader_ip, std::move(empty_dispatcher), callbacks);
        }

        cout << "Finished constructing/joining ManagedGroup" << endl;

        for(int i = 0; i < num_messages; ++i) {
            // random message size between 1 and 100
            unsigned int msg_size = (rand() % 7 + 2) * 10;
            char *buf = managed_group->get_sendbuffer_ptr(msg_size);
            //        cout << "After getting sendbuffer for message " << i <<
            //        endl;
            //        managed_group.debug_print_status();
            while(!buf) {
                buf = managed_group->get_sendbuffer_ptr(msg_size);
            }
            for(unsigned int j = 0; j < msg_size; ++j) {
                buf[j] = 'a' + (i % 26);
            }
            cout << "Client telling DerechoGroup to send message " << i
                 << " with size " << msg_size << endl;
            ;
            managed_group->send();
        }
        while(!done) {
        }

        managed_group->barrier_sync();

        managed_group->leave();

    } catch(const std::exception &e) {
        cout << "Main got an exception: " << e.what() << endl;
        throw e;
    }

    cout << "Finished destroying managed_group" << endl;
}
