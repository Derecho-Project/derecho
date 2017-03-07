#include <chrono>
#include <ratio>
#include <cstdlib>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <vector>
#include <fstream>

#include "derecho/derecho.h"
#include "derecho/logger.h"
#include "initialize.h"

using namespace std;
using namespace std::chrono_literals;
using std::chrono::high_resolution_clock;
using std::chrono::duration;
using std::chrono::microseconds;
using derecho::RawObject;

const int GMS_PORT = 12345;
const size_t message_size = 1000;
const size_t block_size = 1000;

uint32_t num_nodes, node_id;
map<uint32_t, std::string> node_addresses;

const int num_messages = 1000;
bool done = false;
shared_ptr<derecho::Group<>> managed_group;

void stability_callback(uint32_t subgroup, int sender_id, long long int index, char* data, long long int size) {
    using namespace derecho;
    util::debug_log().log_event(stringstream() << "Global stability for message "
                                               << index << " from sender " << sender_id);
}

void persistence_callback(uint32_t subgroup, int sender_id, long long int index, char* data, long long int size) {
    using namespace derecho;
    util::debug_log().log_event(stringstream() << "Persistence complete for message "
                                               << index << " from sender " << sender_id);
    if(index == num_messages - 1 && sender_id == (int)num_nodes - 1) {
        cout << "Done" << endl;
        done = true;
    }
}

void send_messages(int count) {
    derecho::RawSubgroup& group_as_subgroup = managed_group->get_subgroup<RawObject>();
    for(int i = 0; i < count; ++i) {
        char* buffer = group_as_subgroup.get_sendbuffer_ptr(message_size);
        while(!buffer) {
            buffer = group_as_subgroup.get_sendbuffer_ptr(message_size);
        }
        memset(buffer, rand() % 256, message_size);
        group_as_subgroup.send();
    }
}

/*
 * This test sends a fixed number of messages in a group with persistence enabled,
 * to ensure that the persistence-to-disk features work.
 */
int main(int argc, char* argv[]) {
    srand(time(nullptr));
    if(argc < 2) {
        cout << "Error: Expected number of nodes in experiment as the first argument."
             << endl;
        return -1;
    }
    num_nodes = std::atoi(argv[1]);
    derecho::ip_addr my_ip;
    derecho::node_id_t leader_id = 0;
    derecho::ip_addr leader_ip;

    query_node_info(node_id, my_ip, leader_ip);

    string log_filename = (std::stringstream() << "events_node" << node_id << ".csv").str();
    string message_filename = (std::stringstream() << "data" << node_id << ".dat").str();

    derecho::CallbackSet callbacks{stability_callback, persistence_callback};
    derecho::DerechoParams param_object{message_size, block_size, message_filename};
    derecho::SubgroupInfo one_raw_group{{{std::type_index(typeid(RawObject)), &derecho::one_subgroup_entire_view}}};

    if(node_id == leader_id) {
        managed_group = make_shared<derecho::Group<>>(
            my_ip, callbacks, one_raw_group, param_object);
    } else {
        managed_group = make_shared<derecho::Group<>>(
            node_id, my_ip, leader_ip, callbacks, one_raw_group);
    }

    cout << "Created group, waiting for others to join." << endl;
    while(managed_group->get_members().size() < (num_nodes - 1)) {
        std::this_thread::sleep_for(1ms);
    }
    cout << "Starting to send messages." << endl;
    send_messages(num_messages);
    while(!done) {
    }
    // managed_group->barrier_sync();
    ofstream logfile(log_filename);
    managed_group->print_log(logfile);

    // Give log time to print before exiting
    std::this_thread::sleep_for(5s);
    managed_group->leave();
}
