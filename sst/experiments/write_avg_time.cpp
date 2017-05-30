#include <fstream>
#include <iostream>
#include <map>

#include "sst/verbs.h"

using std::ofstream;
using std::map;
using std::cin;
using std::cout;
using std::endl;
using std::string;

using namespace sst;

// number of reruns
long long int num_reruns = 10000;

int main() {
    ofstream fout;
    fout.open("data_write_avg_time", ofstream::app);

    cout << "FOR THIS EXPERIMENT TO WORK, DO NOT START THE POLLING THREAD!!!" << endl;

    // input number of nodes and the local node id
    int num_nodes, node_rank;
    cin >> node_rank;
    cin >> num_nodes;

    // input the ip addresses
    map<uint32_t, string> ip_addrs;
    for(int i = 0; i < num_nodes; ++i) {
        cin >> ip_addrs[i];
    }

    // initialize the rdma resources
    verbs_initialize(ip_addrs, node_rank);

    int r_index = num_nodes - 1 - node_rank;
    for(int size = 1; size <= 10000; ++size) {
        // create buffer for write and read
        char *write_buf, *read_buf;
        write_buf = (char *)malloc(size);
        read_buf = (char *)malloc(size);

        resources res(r_index, read_buf, write_buf, size, size);

        // start the timing experiment
        struct timespec start_time;
        struct timespec end_time;
        long long int nanoseconds_elapsed;

        if(node_rank == 0) {
            clock_gettime(CLOCK_REALTIME, &start_time);
            for(int i = 0; i < num_reruns; ++i) {
                // write the entire buffer
                res.post_remote_write_with_completion(0, size);
                // poll for completion
                verbs_poll_completion();
            }
            clock_gettime(CLOCK_REALTIME, &end_time);
            nanoseconds_elapsed = (end_time.tv_sec - start_time.tv_sec) * 1000000000 + (end_time.tv_nsec - start_time.tv_nsec);
            fout << size << " " << (nanoseconds_elapsed + 0.0) / (1000 * num_reruns) << endl;
            free(write_buf);
            free(read_buf);
        }
        sync(r_index);
    }
    sync(r_index);
    fout.close();
    return 0;
}
