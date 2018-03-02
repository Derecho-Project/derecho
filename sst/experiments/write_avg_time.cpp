#include <chrono>
#include <fstream>
#include <iostream>
#include <map>
#include <vector>

#include "compute_nodes_list.h"
#include "sst/sst.h"
#ifdef USE_VERBS_API
#include "sst/verbs.h"
#else
#include "sst/lf.h"
#endif

using namespace sst;

class ResultSST : public SST<ResultSST> {
public:
    SSTFieldVector<double> avg_times;
    ResultSST(const SSTParams &params)
            : SST<ResultSST>(this, params),
              avg_times(params.members.size()) {
        SSTInit(avg_times);
    }
};

// number of reruns
long long int num_reruns = 10000;

int main() {
    std::vector<int> size_arr = {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384};

    // std::cout << "FOR THIS EXPERIMENT TO WORK, DO NOT START THE POLLING THREAD!!!" << std::endl;
    shutdown_polling_thread();

    // input number of nodes and the local node id
    int num_nodes, node_rank;
    std::cin >> node_rank;
    std::cin >> num_nodes;

    // input the ip addresses
    std::map<uint32_t, std::string> ip_addrs;
    for(int i = 0; i < num_nodes; ++i) {
        std::cin >> ip_addrs[i];
    }

    std::vector<std::vector<double>> avg_times(num_nodes);
    for(int i = 0; i < num_nodes; ++i) {
        avg_times[i].resize(size_arr.size(), 0.0);
    }

    // initialize the rdma resources
#ifdef USE_VERBS_API
    verbs_initialize(ip_addrs, node_rank);
#else
    lf_initialize(ip_addrs, node_rank);
#endif

    auto nodes_list = compute_nodes_list(node_rank, num_nodes);
    for(auto remote_rank : nodes_list) {
        sync(remote_rank);
        for(uint j = 0; j < size_arr.size(); ++j) {
            int size = size_arr[j];
            // create buffer for write and read
            char *write_buf, *read_buf;
            write_buf = (char *)malloc(size);
            read_buf = (char *)malloc(size);

            resources res(remote_rank, read_buf, write_buf, size, size, remote_rank > node_rank);

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
#ifdef USE_VERBS_API
                    verbs_poll_completion();
#else
                    lf_poll_completion();
#endif
                }
                clock_gettime(CLOCK_REALTIME, &end_time);
                nanoseconds_elapsed = (end_time.tv_sec - start_time.tv_sec) * 1000000000 + (end_time.tv_nsec - start_time.tv_nsec);
                avg_times[remote_rank][j] = (nanoseconds_elapsed + 0.0) / (1000 * num_reruns);
                // fout << node_rank << " " << remote_rank << " " << size << " " << (nanoseconds_elapsed + 0.0) / (1000 * num_reruns) << std::endl;
                free(write_buf);
                free(read_buf);
            }
            sync(remote_rank);
        }
        for(uint j = 0; j < size_arr.size(); ++j) {
            int size = size_arr[j];
            // create buffer for write and read
            char *write_buf, *read_buf;
            write_buf = (char *)malloc(size);
            read_buf = (char *)malloc(size);

            resources res(remote_rank, read_buf, write_buf, size, size, remote_rank > node_rank);

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
#ifdef USE_VERBS_API
                    verbs_poll_completion();
#else
                    lf_poll_completion();
#endif
                }
                clock_gettime(CLOCK_REALTIME, &end_time);
                nanoseconds_elapsed = (end_time.tv_sec - start_time.tv_sec) * 1000000000 + (end_time.tv_nsec - start_time.tv_nsec);
                avg_times[remote_rank][j] = (nanoseconds_elapsed + 0.0) / (1000 * num_reruns);
                // fout << node_rank << " " << remote_rank << " " << size << " " << (nanoseconds_elapsed + 0.0) / (1000 * num_reruns) << std::endl;
                free(write_buf);
                free(read_buf);
            }
            sync(remote_rank);
        }
    }
    for(int i = 0; i < num_nodes; ++i) {
        if(i != node_rank) {
            sync(i);
        }
    }

    std::ofstream fout;
    if(node_rank == 0) {
        fout.open("data_write_avg_time");
    }
    std::vector<uint32_t> members(num_nodes);
    for(int i = 0; i < num_nodes; ++i) {
        members[i] = i;
    }
    std::cout << "Printing average times" << std::endl;
    for (int i = 0; i < num_nodes; ++i) {
      for (uint j = 0; j < size_arr.size(); ++j) {
          std::cout << avg_times[i][j] << " ";
      }
      std::cout << std::endl;
    }
    ResultSST sst(SSTParams(members, node_rank));
    for(uint k = 0; k < size_arr.size(); ++k) {
        int size = size_arr[k];
        for(int j = 0; j < num_nodes; ++j) {
            sst.avg_times[node_rank][j] = avg_times[j][k];
	    std::cout << sst.avg_times[node_rank][j] << " ";
        }
	std::cout << std::endl;
	sst.put();
        sst.sync_with_members();
        if(node_rank == 0) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            fout << size << std::endl;
            for(int i = 0; i < num_nodes; ++i) {
                for(int j = 0; j < num_nodes; ++j) {
                    fout << sst.avg_times[i][j] << " ";
                }
                fout << std::endl;
            }
            fout << std::endl;
        }
        sst.sync_with_members();
    }
    if(node_rank == 0) {
        fout.close();
    }

    return 0;
}
