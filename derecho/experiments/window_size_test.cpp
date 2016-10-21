#include <iostream>
#include <fstream>
#include <vector>
#include <time.h>

#include "../derecho_group.h"
#include "../rdmc/rdmc.h"
#include "block_size.h"
#include "aggregate_bandwidth.h"
#include "log_results.h"
#include "initialize.h"

using std::cout;
using std::endl;
using std::cin;
using std::vector;
using derecho::DerechoGroup;
using derecho::DerechoRow;

constexpr int MAX_GROUP_SIZE = 8;

int main(int argc, char *argv[]) {
    srand(time(NULL));

    uint32_t node_rank;
    uint32_t num_nodes;
    std::map<uint32_t, std::string> node_address_map = initialize(node_rank, num_nodes);

    vector<uint32_t> members(num_nodes);
    for(int i = 0; i < (int)num_nodes; ++i) {
        members[i] = i;
    }

    long long unsigned int msg_size = atoll(argv[1]);
    unsigned int window_size = atoll(argv[2]);
    long long unsigned int block_size = get_block_size(msg_size);
    int num_messages = 1000;

    bool done = false;
    auto stability_callback = [&num_messages, &done, &num_nodes](
        int sender_id, long long int index, char *buf, long long int msg_size) {
        if(index == num_messages - 1 && sender_id == (int)num_nodes - 1) {
            cout << "Done" << endl;
            done = true;
        }
    };

    std::shared_ptr<sst::SST<DerechoRow<MAX_GROUP_SIZE>, sst::Mode::Writes>>
        derecho_sst =
            std::make_shared<sst::SST<DerechoRow<8>, sst::Mode::Writes>>(
                members, node_rank);
    vector<derecho::MessageBuffer> free_message_buffers;
    DerechoGroup<MAX_GROUP_SIZE, Dispatcher<>> g(
        members, node_rank, derecho_sst, free_message_buffers, Dispatcher<>(node_rank),
        derecho::CallbackSet{stability_callback, nullptr},
        derecho::DerechoParams{msg_size, block_size, "", window_size},
        node_address_map);

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
    long long int nanoseconds_elapsed =
        (end_time.tv_sec - start_time.tv_sec) * (long long int)1e9 +
        (end_time.tv_nsec - start_time.tv_nsec);
    double bw =
        (msg_size * num_messages * num_nodes * 8 + 0.0) / nanoseconds_elapsed;
    double avg_bw = aggregate_bandwidth(members, node_rank, bw);
    struct params {
        long long unsigned int msg_size;
        unsigned int window_size;
        double avg_bw;

        void print(std::ofstream &fout) {
            fout << msg_size << " " << window_size << " " << avg_bw << endl;
        }
    } t{msg_size, window_size, avg_bw};
    log_results(t, "data_window_size");
    return 0;
}
