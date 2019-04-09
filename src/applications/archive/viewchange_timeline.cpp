#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <map>
#include <ratio>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <derecho/core/derecho.hpp>
#include "initialize.hpp"
#include <derecho/utils/time.h>

using namespace std;
using namespace std::chrono_literals;
using std::chrono::duration;
using std::chrono::high_resolution_clock;
using std::chrono::microseconds;

const uint64_t SECOND = 1000000000ull;
const size_t message_size = 200000000;
const size_t block_size = 1000000;

uint32_t num_nodes, node_id;
map<uint32_t, std::string> node_addresses;

shared_ptr<derecho::Group<>> managed_group;

void stability_callback(uint32_t subgroup, uint32_t sender_id, long long int index, char *data,
                        long long int size) {
    std::stringstream string_formatter;
    string_formatter << "Message " << index << " from sender " << sender_id << " delivered";
    whenlog(managed_group->log_event(string_formatter.str());)
}

void send_messages(uint64_t duration) {
    uint64_t end_time = get_time() + duration;
    while(get_time() < end_time) {
        char *buffer = managed_group->get_subgroup<derecho::RawObject>().get_sendbuffer_ptr(message_size);
        if(buffer) {
            memset(buffer, rand() % 256, message_size);
            //          cout << "Send function call succeeded at the client
            //          side" << endl;
            managed_group->get_subgroup<derecho::RawObject>().send();
        }
    }
}

int main(int argc, char *argv[]) {
    const long long unsigned int sst_message_size = (message_size < 17000 ? message_size : 0);
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

    // Synchronize clocks
    vector<uint32_t> members;
    for(uint32_t i = 0; i < num_nodes; i++) members.push_back(i);
    auto universal_barrier_group = make_unique<rdmc::barrier_group>(members);

    universal_barrier_group->barrier_wait();
    auto t1 = high_resolution_clock::now();
    universal_barrier_group->barrier_wait();
    auto t2 = high_resolution_clock::now();
    //    derecho::program_start_time = high_resolution_clock::now();
    universal_barrier_group->barrier_wait();
    auto t3 = high_resolution_clock::now();

    printf(
            "Synchronized clocks.\nTotal possible variation = %5.3f us\n"
            "Max possible variation from local = %5.3f us\n",
            duration<double, std::micro>(t3 - t1).count(),
            duration<double, std::micro>(max((t2 - t1), (t3 - t2))).count());
    fflush(stdout);
    cout << endl
         << endl;

    using derecho::RawObject;

    derecho::CallbackSet callbacks{stability_callback, nullptr};
    derecho::DerechoParams param_object{message_size, sst_message_size, block_size};
    derecho::SubgroupInfo one_raw_group{{{std::type_index(typeid(RawObject)), &derecho::one_subgroup_entire_view}},
                                        {std::type_index(typeid(RawObject))}};

    if(node_id == num_nodes - 1) {
        cout << "Sleeping for 10 seconds..." << endl;
        std::this_thread::sleep_for(10s);
        cout << "Connecting to group" << endl;
        managed_group = make_shared<derecho::Group<>>(
                node_id, my_ip, leader_ip, callbacks, one_raw_group);
        whenlog(managed_group->log_event("About to start sending");)
        send_messages(10 * SECOND);
        whenlog(managed_group->log_event("About to exit");)
        exit(0);
    } else {
        if(node_id == leader_id) {
            managed_group = make_shared<derecho::Group<>>(
                    node_id, my_ip, callbacks, one_raw_group, param_object);
        } else {
            managed_group = make_shared<derecho::Group<>>(
                    node_id, my_ip, leader_ip, callbacks, one_raw_group);
        }
        cout << "Created group, waiting for others to join." << endl;
        while(managed_group->get_members().size() < (num_nodes - 1)) {
            std::this_thread::sleep_for(1ms);
        }
        send_messages(30 * SECOND);
        // managed_group->barrier_sync();
        std::this_thread::sleep_for(5s);
        managed_group->leave();
    }
}
