
#include "rdmc/util.h"
#include "rdmc/message.h"
#include "rdmc/verbs_helper.h"
#include "rdmc/rdmc.h"
#include "rdmc/microbenchmarks.h"
#include "rdmc/group_send.h"
#include "sst/sst.h"
#include "sst/predicates.h"

#include <atomic>
#include <algorithm>
#include <cassert>
#include <condition_variable>
#include <cinttypes>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <sys/mman.h>
#include <sys/resource.h>
#include <thread>
#include <vector>

using namespace std;
using namespace sst;
uint32_t node_rank;
uint32_t num_nodes;
map<uint32_t, string> node_addresses;

struct Row {
    volatile bool done[1000];
};

int main() {
    query_addresses(node_addresses, node_rank);
    num_nodes = node_addresses.size();

    // initialize RDMA resources, input number of nodes, node rank and ip
    // addresses and create TCP connections
    rdmc::initialize(node_addresses, node_rank);

    // initialize the rdma resources
    sst::verbs_initialize(node_rank, node_addresses);

    // set block size
    const size_t block_size = 10;
    // all the nodes are in the group
    const uint16_t group_size = num_nodes;
    // maximum outstanding messages in the pipleline so that we can still send
    // the
    // next message
    int num_out = 10;
    // size of one message
    int msg_size = 1000;
    // size of the buffer
    const size_t buffer_size = num_out * msg_size;
    // buffer for the message - received here by the receivers and generated
    // here
    // by the sender
    unique_ptr<char[]> buffer(new char[buffer_size]);
    auto mr = make_shared<rdma::memory_region>(buffer.get(), buffer_size);

    // create the vector of members - node 0 is the sender
    vector<uint32_t> members(group_size);
    for(uint32_t i = 0; i < group_size; i++) {
        members[i] = i;
    }

    // create a new shared state table with all the members
    SST<Row, Mode::Writes> *sst =
        new SST<Row, Mode::Writes>(members, node_rank);

    for(int i = 0; i < 1000; ++i) {
        (*sst)[node_rank].done[i] = false;
    }
    sst->put();
    sst->sync_with_members();

    int cur_msg = 0;

    // Map from (group_size, block_size, send_type) to group_number.
    map<std::tuple<uint16_t, size_t, rdmc::send_algorithm>, uint16_t>
        send_groups;
    // type of send algorithm
    rdmc::send_algorithm type = rdmc::BINOMIAL_SEND;
    // parameters of the group
    std::tuple<uint16_t, size_t, rdmc::send_algorithm> send_params(
        group_size, block_size, type);
    // identifier of the group
    int group_number = 0;
    // create the group
    rdmc::create_group(
        group_number, members, block_size, type,
        [&](size_t length) -> rdmc::receive_destination {
            return {mr, (size_t)msg_size * ((size_t)cur_msg % 10)};
        },
        [&](char *data, size_t size) {
            cout << (void *)data << endl;
            (*sst)[node_rank].done[cur_msg] = true;
            sst->put();
            cur_msg++;
            cout << "Done" << endl;
        },
        [](optional<uint32_t>) {});
    send_groups.emplace(send_params, 0);

    auto f = [&](const SST<Row, Mode::Writes> &sst) {
        cout << "In predicate : " << endl;
        cout << "cur_msg is : " << cur_msg << endl;
        cout << endl;
        if(cur_msg < num_out) {
            return true;
        }
        for(uint16_t i = 0; i < num_nodes; ++i) {
            if(sst[i].done[cur_msg - num_out] != true) {
                return false;
            }
        }
        return true;
    };

    auto g = [&](SST<Row, Mode::Writes> &sst) {
        cout << "In trigger : " << endl;
        cout << "cur_msg is : " << cur_msg << endl;
        cout << endl;
        cout << "Reached here" << endl;
        cout << msg_size *(cur_msg % 10) << endl;
        // send the message
        rdmc::send(group_number, mr, msg_size * (cur_msg % 10), msg_size);
        // done is set to true upon completion
        while(!sst[node_rank].done[cur_msg]) {
        }
        if(cur_msg == 1000) {
            while(true) {
                bool complete = true;
                for(uint16_t i = 0; i < num_nodes; ++i) {
                    complete = complete & sst[i].done[cur_msg - 1];
                }
                if(complete) {
                    break;
                }
            }
            sst.sync_with_members();
            rdmc::shutdown();
        }
    };

    // code for the receivers
    if(node_rank > 0) {
        // sync to know when the sender is finished
        sst->sync_with_members();
        rdmc::shutdown();
    }
    // sender code
    else {
        // generate the message
        for(size_t i = 0; i < buffer_size; i += 1) {
            buffer[i] = (rand() >> 5) % 26 + 'a';
        }
        // register the predicate
        sst->predicates.insert(f, g, PredicateType::RECURRENT);
        while(true) {
        }
    }
}
