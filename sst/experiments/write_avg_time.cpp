#include <fstream>
#include <iostream>
#include <map>
#include <vector>

#include "compute_nodes_list.h"
#include "sst/verbs.h"

using namespace sst;

// number of reruns
long long int num_reruns = 10000;

int main() {
    std::ofstream fout;
    fout.open("data_write_avg_time", std::ofstream::app);
    std::vector<int> size_arr = {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384};

    std::cout << "FOR THIS EXPERIMENT TO WORK, DO NOT START THE POLLING THREAD!!!" << std::endl;

    // input number of nodes and the local node id
    int num_nodes, node_rank;
    std::cin >> node_rank;
    std::cin >> num_nodes;

    // input the ip addresses
    std::map<uint32_t, std::string> ip_addrs;
    for(int i = 0; i < num_nodes; ++i) {
        std::cin >> ip_addrs[i];
    }

    // initialize the rdma resources
    verbs_initialize(ip_addrs, node_rank);

    auto nodes_list = compute_nodes_list(node_rank, num_nodes);
    for(auto remote_rank : nodes_list) {
        sync(remote_rank);
        for(int size : size_arr) {
            // create buffer for write and read
            char *write_buf, *read_buf;
            write_buf = (char *)malloc(size);
            read_buf = (char *)malloc(size);

            resources res(remote_rank, read_buf, write_buf, size, size);

            // start the timing experiment
            struct timespec start_time;
            struct timespec end_time;
            long long int nanoseconds_elapsed;

            if(node_rank < remote_rank) {
                clock_gettime(CLOCK_REALTIME, &start_time);
                for(int i = 0; i < num_reruns; ++i) {
                    // write the entire buffer
                    res.post_remote_write_with_completion(0, size);
                    // poll for completion
                    verbs_poll_completion();
                }
                clock_gettime(CLOCK_REALTIME, &end_time);
                nanoseconds_elapsed = (end_time.tv_sec - start_time.tv_sec) * 1000000000 + (end_time.tv_nsec - start_time.tv_nsec);
                fout << node_rank << " " << remote_rank << " " << size << " " << (nanoseconds_elapsed + 0.0) / (1000 * num_reruns) << std::endl;
                free(write_buf);
                free(read_buf);
            }
            sync(remote_rank);
        }
        for(int size : size_arr) {
            // create buffer for write and read
            char *write_buf, *read_buf;
            write_buf = (char *)malloc(size);
            read_buf = (char *)malloc(size);

            resources res(remote_rank, read_buf, write_buf, size, size);

            // start the timing experiment
            struct timespec start_time;
            struct timespec end_time;
            long long int nanoseconds_elapsed;

            if(remote_rank < node_rank) {
                clock_gettime(CLOCK_REALTIME, &start_time);
                for(int i = 0; i < num_reruns; ++i) {
                    // write the entire buffer
                    res.post_remote_write_with_completion(0, size);
                    // poll for completion
                    verbs_poll_completion();
                }
                clock_gettime(CLOCK_REALTIME, &end_time);
                nanoseconds_elapsed = (end_time.tv_sec - start_time.tv_sec) * 1000000000 + (end_time.tv_nsec - start_time.tv_nsec);
                fout << node_rank << " " << remote_rank << " " << size << " " << (nanoseconds_elapsed + 0.0) / (1000 * num_reruns) << std::endl;
                free(write_buf);
                free(read_buf);
            }
            sync(remote_rank);
        }
    }
    for (int i = 0; i < num_nodes; ++i) {
      if (i != node_rank) {
	sync(i);
      }
    }
    fout.close();
    return 0;
}
