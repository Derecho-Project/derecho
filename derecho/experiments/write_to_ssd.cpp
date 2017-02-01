#include <iostream>
#include <fstream>
#include <vector>
#include <time.h>

#include "derecho/multicast_group.h"
#include "derecho/derecho_caller.h"
#include "block_size.h"
#include "aggregate_bandwidth.h"
#include "log_results.h"
#include "initialize.h"

using std::cout;
using std::endl;
using std::cin;
using std::vector;
using derecho::MulticastGroup;
using derecho::DerechoSST;

constexpr int MAX_GROUP_SIZE = 8;

int main(int argc, char *argv[]) {
    srand(time(NULL));

    uint32_t node_rank;
    uint32_t num_nodes;

    std::map<uint32_t, std::string> node_address_map = initialize(node_rank, num_nodes);

    vector<uint32_t> members(num_nodes);
    for(uint32_t i = 0; i < num_nodes; ++i) {
        members[i] = i;
    }

    long long unsigned int msg_size = atoll(argv[1]);
    long long unsigned int block_size = get_block_size(msg_size);
    int num_messages = 1000;

    std::ofstream fssd;
    fssd.open("messages");
    bool done = false;
    auto stability_callback = [&fssd, &num_messages, &done, &num_nodes](
        int sender_id, long long int index, char *buf, long long int msg_size) {
        fssd.write(buf, msg_size);
        fssd.flush();
        if(index == num_messages - 1 && sender_id == (int)num_nodes - 1) {
            cout << "Done" << endl;
            done = true;
        }
    };

    auto derecho_sst =
        std::make_shared<DerechoSST>(sst::SSTParams(members, node_rank));
    vector<derecho::MessageBuffer> free_message_buffers;
    MulticastGroup<rpc::Dispatcher<>> g(
        members, node_rank, derecho_sst, free_message_buffers,
        rpc::Dispatcher<>(node_rank), derecho::CallbackSet{stability_callback, nullptr},
        derecho::DerechoParams{msg_size, block_size}, node_address_map);
    struct timespec start_time;
    // start timer
    clock_gettime(CLOCK_REALTIME, &start_time);
    for(int i = 0; i < num_messages; ++i) {
        char *buf = g.get_position(msg_size);
        while(!buf) {
            buf = g.get_position(msg_size);
        }
        g.send();
    }
    while(!done) {
    }
    struct timespec end_time;
    clock_gettime(CLOCK_REALTIME, &end_time);
    fssd.close();
    long long int nanoseconds_elapsed =
        (end_time.tv_sec - start_time.tv_sec) * (long long int)1e9 +
        (end_time.tv_nsec - start_time.tv_nsec);
    double bw =
        (msg_size * num_messages * num_nodes * 8 + 0.0) / nanoseconds_elapsed;
    double avg_bw = aggregate_bandwidth(members, node_rank, bw);
    log_results(msg_size, avg_bw, "data_ssd_bw");
}
