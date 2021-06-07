#include <fstream>
#include <iostream>
#include <time.h>
#include <vector>

#include "aggregate_bandwidth.hpp"
#include "block_size.hpp"
#include <derecho/core/derecho.hpp>
#include "initialize.hpp"
#include "log_results.hpp"

using derecho::RawObject;
using std::cin;
using std::cout;
using std::endl;
using std::vector;

constexpr int MAX_GROUP_SIZE = 8;

struct exp_result {
    long long unsigned int msg_size;
    double bw;

    void print(std::ofstream &fout) {
        fout << msg_size << " " << bw << endl;
    }
};

int main(int argc, char *argv[]) {
    srand(time(NULL));

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

    long long unsigned int msg_size = atoll(argv[1]);
    long long unsigned int block_size = get_block_size(msg_size);
    const long long unsigned int sst_msg_size = (msg_size < 17000 ? msg_size : 0);

    int num_messages = 1000;

    std::ofstream fssd;
    fssd.open("messages");
    bool done = false;
    auto stability_callback = [&fssd, &num_messages, &done, &num_nodes](
                                      uint32_t subgroup, int sender_id, long long int index, char *buf, long long int msg_size) {
        fssd.write(buf, msg_size);
        fssd.flush();
        if(index == num_messages - 1 && sender_id == (int)num_nodes - 1) {
            cout << "Done" << endl;
            done = true;
        }
    };

    derecho::SubgroupInfo one_raw_group{{{std::type_index(typeid(RawObject)), &derecho::one_subgroup_entire_view}},
                                        {std::type_index(typeid(RawObject))}};
    std::unique_ptr<derecho::Group<>> g;
    if(my_ip == leader_ip) {
        g = std::make_unique<derecho::Group<>>(
                node_id, my_ip, derecho::UserMessageCallbacks{stability_callback, nullptr},
                one_raw_group,
                derecho::DerechoParams{msg_size, sst_msg_size, block_size});
    } else {
        g = std::make_unique<derecho::Group<>>(
                node_id, my_ip, leader_ip,
                derecho::UserMessageCallbacks{stability_callback, nullptr},
                one_raw_group);
    }

    derecho::RawSubgroup &sg = g->get_subgroup<RawObject>();

    struct timespec start_time;
    // start timer
    clock_gettime(CLOCK_REALTIME, &start_time);
    for(int i = 0; i < num_messages; ++i) {
        char *buf = sg.get_sendbuffer_ptr(msg_size);
        while(!buf) {
            buf = sg.get_sendbuffer_ptr(msg_size);
        }
        sg.send();
    }
    while(!done) {
    }
    struct timespec end_time;
    clock_gettime(CLOCK_REALTIME, &end_time);
    fssd.close();
    long long int nanoseconds_elapsed = (end_time.tv_sec - start_time.tv_sec) * (long long int)1e9 + (end_time.tv_nsec - start_time.tv_nsec);
    double bw = (msg_size * num_messages * num_nodes * 8 + 0.0) / nanoseconds_elapsed;
    double avg_bw = aggregate_bandwidth(g->get_members(), node_id, bw);
    log_results(exp_result{msg_size, avg_bw}, "data_ssd_bw");
}
