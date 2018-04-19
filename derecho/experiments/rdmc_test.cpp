#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <time.h>
#include <vector>

#include "aggregate_bandwidth.h"
#include "block_size.h"
#include "boost/optional.hpp"
#include "log_results.h"
#include "rdmc/rdmc.h"
#include "rdmc/util.h"

using std::vector;

int main(int argc, char *argv[]) {
    uint32_t node_rank;
    uint32_t num_nodes;

    std::map<uint32_t, std::string> node_addresses;

    rdmc::query_addresses(node_addresses, node_rank);
    num_nodes = node_addresses.size();

    // initialize RDMA resources, input number of nodes, node rank and ip addresses and create TCP connections
    bool success = rdmc::initialize(node_addresses, node_rank);
    if(!success) {
        std::cout << "Failed RDMC initialization" << std::endl;
        std::cout << "Exiting" << std::endl;
    }
    // size of one message
    long long int msg_size = 100;
    // set block size
    const size_t block_size = get_block_size(msg_size);
    // size of the buffer
    const size_t buffer_size = msg_size;

    // create the vector of members - node 0 is the sender
    vector<uint32_t> members(num_nodes);
    for(uint32_t i = 0; i < num_nodes; i++) {
        members[i] = i;
    }

    volatile int count = 0;
    int num_messages = 10;
    // type of send algorithm
    rdmc::send_algorithm type = rdmc::BINOMIAL_SEND;

    // buffer for the message - received here by the receivers and generated
    // here by the sender
    std::unique_ptr<char[]> buffer(new char[buffer_size]);
    auto mr = std::make_shared<rdma::memory_region>(buffer.get(), buffer_size);

    // create the group
    success = rdmc::create_group(0, members, block_size, type,
                                 [&mr](size_t length) -> rdmc::receive_destination {
                                     std::cout << "Incoming message callback for message of length " << length << std::endl;
                                     return {mr, 0};
                                 },
                                 [&count](char *data, size_t size) {
                                     std::cout << "Received message number " << count << " of size " << size << std::endl;
                                     std::cout << "Message contents" << std::endl;
                                     for(uint i = 0; i < size; ++i) {
                                         std::cout << data[i];
                                     }
                                     std::cout << std::endl;
                                     ++count;
                                 },
                                 [](std::experimental::optional<uint32_t>) {});
    if(!success) {
        std::cout << "Failed RDMC group creation" << std::endl;
        std::cout << "Exiting" << std::endl;
    }

    struct timespec start_time, end_time;
    clock_gettime(CLOCK_REALTIME, &start_time);
    // generate random data in the buffer
    srand(time(NULL));
    if(node_rank == 0) {
        for(int i = 0; i < num_messages; ++i) {
            for(uint i = 0; i < buffer_size; ++i) {
	      buffer[i] = rand() % 26 + 'a';
            }
            // send the message
            success = rdmc::send(node_rank, mr, 0, msg_size);
            while(count <= i) {
            }
        }
    } else {
        while(count <= num_messages - 1) {
        }
    }
    clock_gettime(CLOCK_REALTIME, &end_time);
}
