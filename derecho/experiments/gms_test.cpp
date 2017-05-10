#include <cstdlib>
#include <iostream>
#include <map>
#include <string>

#include "block_size.h"
#include "derecho/derecho.h"
#include "initialize.h"

using std::string;
using std::cin;
using std::cout;
using std::endl;
using std::map;

using derecho::RawObject;

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

        long long unsigned int max_msg_size = 1000000;
        long long unsigned int block_size = 100000;

        int num_messages = 100000;
        int received_count = 0;

        bool done = false;
        auto stability_callback = [&num_messages, &done, &received_count](
                uint32_t subgroup, uint32_t sender_id, long long int index, char *buf,
                long long int msg_size) {
            received_count++;
            if(received_count % 1000 == 0) {
                cout << "In stability callback; sender = " << sender_id
                     << ", index = " << index << endl;
            }
            if(received_count == num_messages) {
                done = true;
            }
        };

        derecho::CallbackSet callbacks{stability_callback, nullptr};
        derecho::DerechoParams param_object{max_msg_size, block_size};
        derecho::SubgroupInfo one_raw_group{{{std::type_index(typeid(RawObject)), &derecho::one_subgroup_entire_view}}};

        std::unique_ptr<derecho::Group<>> managed_group;

        if(node_id == leader_id) {
            assert(my_ip == leader_ip);
            managed_group = std::make_unique<derecho::Group<>>(
                    node_id, my_ip, callbacks, one_raw_group, param_object);
        } else {
            managed_group = std::make_unique<derecho::Group<>>(
                    node_id, my_ip, leader_ip, callbacks, one_raw_group);
        }

        cout << "Finished constructing/joining ManagedGroup" << endl;

        while(managed_group->get_members().size() < num_nodes) {
        }

        for(int i = 0; i < num_messages; ++i) {
            derecho::RawSubgroup &group_as_subgroup = managed_group->get_subgroup<RawObject>();
            // random message size between 1 and 100
            unsigned int msg_size = (rand() % 7 + 2) * (max_msg_size / 10);
            char *buf = group_as_subgroup.get_sendbuffer_ptr(msg_size);
            //          cout << "After getting sendbuffer for message " << i <<  endl;
            //          managed_group.debug_print_status();
            while(!buf) {
                buf = group_as_subgroup.get_sendbuffer_ptr(msg_size);
            }
            for(unsigned int j = 0; j < msg_size; ++j) {
                buf[j] = 'a' + i;
            }
            //          cout << "Client telling DerechoGroup to send message " << i << " with size " << msg_size << endl;
            group_as_subgroup.send();
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
