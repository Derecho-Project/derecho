
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <map>
#include <ratio>
#include <string>
#include <thread>
#include <vector>

#include "time/time.h"
#include "derecho/derecho.h"
#include "rdmc/util.h"
#include "initialize.h"

using namespace std;
using namespace std::chrono_literals;
using std::chrono::high_resolution_clock;
using std::chrono::duration;
using std::chrono::microseconds;
using derecho::RawObject;

const int GMS_PORT = 12345;
const uint64_t SECOND = 1000000000ull;
const size_t message_size = 200000000;
const size_t block_size = 1000000;

uint32_t num_nodes, node_id;
map<uint32_t, std::string> node_addresses;

unsigned int message_number = 0;
vector<uint64_t> message_times;
shared_ptr<derecho::Group<>> managed_group;

void stability_callback(uint32_t subgroup, int sender_id, long long int index, char *data, long long int size) {
    using namespace derecho;
    message_times.push_back(get_time());

    stringstream string_formatter;
    string_formatter << "Global stability for message " << index << " from sender " << sender_id;
    managed_group->log_event(string_formatter.str());

    while(!managed_group) {
    }

    unsigned int n = managed_group->get_members().size();
    if(message_number >= n) {
        unsigned int dt =
            message_times.back() - message_times[message_number - n];
        double bandwidth = (message_size * n * 8.0) / dt;
        managed_group->log_event(std::to_string(bandwidth));
    }

    ++message_number;
}

void send_messages(uint64_t duration) {
    uint64_t end_time = get_time() + duration;
    while(get_time() < end_time) {
        derecho::RawSubgroup &group_as_subgroup = managed_group->get_subgroup<RawObject>();
        char *buffer = group_as_subgroup.get_sendbuffer_ptr(message_size);
        if(buffer) {
            memset(buffer, rand() % 256, message_size);
            //          cout << "Send function call succeeded at the client
            //          side" << endl;
            group_as_subgroup.send();
        }
    }
}

/*
 * This test runs a group of nodes for 30 seconds of continuous sending with no
 * failures. It tests the bandwidth of a ManagedGroup in the "steady state."
 */
int main(int argc, char *argv[]) {
    srand(time(nullptr));
    if(argc < 2) {
        cout << "Error: Expected number of nodes in experiment as the first argument."
             << endl;
        return -1;
    }
    uint32_t num_nodes = std::atoi(argv[1]);
    derecho::node_id_t node_id;
    derecho::ip_addr my_ip;
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

    derecho::CallbackSet callbacks{stability_callback, nullptr};
    derecho::DerechoParams param_object{message_size, block_size};
    derecho::SubgroupInfo one_raw_group{{{std::type_index(typeid(RawObject)), &derecho::one_subgroup_entire_view}}};

    if(my_ip == leader_ip) {
        managed_group = std::make_unique<derecho::Group<>>(
            node_id, my_ip, callbacks, one_raw_group, param_object);
    } else {
        managed_group = std::make_unique<derecho::Group<>>(
            node_id, my_ip, leader_ip, callbacks, one_raw_group);
    }

    cout << "Created group, waiting for others to join." << endl;
    while(managed_group->get_members().size() < (num_nodes - 1)) {
        std::this_thread::sleep_for(1ms);
    }
    cout << "Starting to send messages." << endl;
    send_messages(30 * SECOND);
    // managed_group->barrier_sync();

    // Give log time to print before exiting
    std::this_thread::sleep_for(5s);
    managed_group->leave();
}
