#include <iostream>
#include <map>
#include <vector>
#include <fstream>

#include "sst/verbs.h"
#include "sst/tcp.h"

using namespace std;
using namespace sst;
using namespace sst::tcp;

namespace sst {
namespace tcp {
extern int port;
}
}

// limit of buffer size for remote read
long long int max_size = 10000;

// step size for size
int step_size = 100;

// number of reruns
long long int num_reruns = 10000;

void initialize(int node_rank, const map <uint32_t, string> & ip_addrs) {
  // initialize tcp connections
  tcp_initialize(node_rank, ip_addrs);
  
  // initialize the rdma resources
  verbs_initialize();
}

int main() {
    ofstream fout[5];

    // input number of nodes and the local node id
    int num_nodes, node_rank;
    cin >> num_nodes;
    cin >> node_rank;

    // input the ip addresses
    map<uint32_t, string> ip_addrs;
    for (int i = 0; i < num_nodes; ++i) {
        cin >> ip_addrs[i];
    }

    cin >> tcp::port;

    // create all tcp connections and initialize global rdma resources
    initialize(node_rank, ip_addrs);

    vector<int> sizes = { 16, 256, 512, 1024, 2048 };

    if (node_rank == 0) {
        fout[0].open("16_data_parallel_read", ofstream::app);
        fout[1].open("256_data_parallel_read", ofstream::app);
        fout[2].open("512_data_parallel_read", ofstream::app);
        fout[3].open("1024_data_parallel_read", ofstream::app);
        fout[4].open("2048_data_parallel_read", ofstream::app);
    }

    int k = 0;
    
    for (auto size : sizes) {
        // only the 0 rank node reads remote data from each of the other processes
        if (node_rank == 0) {
            // create an array of resources
            resources *res[num_nodes];
            // create buffer for write and read
            char *write_buf[num_nodes], *read_buf[num_nodes];
            for (int r_index = 1; r_index < num_nodes; ++r_index) {
                write_buf[r_index] = (char *) malloc(size);
                read_buf[r_index] = (char *) malloc(size);

                res[r_index] = new resources(r_index, write_buf[r_index],
                        read_buf[r_index], size, size);
            }

            // start the timing experiment
            struct timespec start_time;
            struct timespec end_time;
            long long int nanoseconds_elapsed;

            clock_gettime(CLOCK_REALTIME, &start_time);

            for (int i = 0; i < num_reruns; ++i) {
                // start sequential reads
                for (int r_index = 1; r_index < num_nodes; ++r_index) {
                    res[r_index]->post_remote_read(size);
                }

                // poll for completion simultaneously
                for (int r_index = 1; r_index < num_nodes; ++r_index) {
                    verbs_poll_completion();
                }

                // // poll all completions in one call
                // poll_completion (num_nodes-1);
            }

            clock_gettime(CLOCK_REALTIME, &end_time);
            nanoseconds_elapsed = (end_time.tv_sec - start_time.tv_sec)
                    * 1000000000 + (end_time.tv_nsec - start_time.tv_nsec);

            fout[k] << num_nodes << ", " << (nanoseconds_elapsed + 0.0) / (1000 * num_reruns) << endl;
            k++;

            // sync to notify other nodes of a finished read
            for (int r_index = 1; r_index < num_nodes; ++r_index) {
                char temp_char;
                char tQ[2] = { 'Q', 0 };
                sock_sync_data(get_socket(r_index), 1, tQ, &temp_char);

                // free malloc()ed area
                free(write_buf[r_index]);
                free(read_buf[r_index]);
                // destroy the resource
                delete (res[r_index]);
            }
        }

        else {
            // create a resource with node 0
            // create buffer for write and read
            char *write_buf, *read_buf;
            write_buf = (char *) malloc(size);
            read_buf = (char *) malloc(size);

            // write to the write buffer
            for (int i = 0; i < size - 1; ++i) {
                write_buf[i] = 'a' + node_rank;
            }
            write_buf[size - 1] = 0;

            resources *res = new resources(0, write_buf, read_buf, size, size);
            char temp_char;
            char tQ[2] = { 'Q', 0 };
            // wait for node 0 to finish read
            sock_sync_data(get_socket(0), 1, tQ, &temp_char);

            // free malloc()ed area
            free(write_buf);
            free(read_buf);
            // destroy the resource
            delete (res);
        }
    }

    return 0;
}
