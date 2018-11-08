#include <fstream>
#include <iostream>
#include <map>
#include <vector>

#include "sst/tcp.h"
#include "sst/verbs.h"

using namespace std;
using namespace sst;
using namespace sst::tcp;

// limit of buffer size for remote read
long long int max_size = 10000;

// number of reruns
long long int num_reruns = 100;

void initialize(int node_rank, const map<uint32_t, string> &ip_addrs) {
    // initialize tcp connections
    tcp_initialize(node_rank, ip_addrs);

    // initialize the rdma resources
    verbs_initialize();
}

int main() {
    ofstream fout;
    fout.open("data_remote_read.csv");
    ofstream fout_bw;
    fout_bw.open("data_remote_read_bw.csv");
    ofstream fout_read;
    fout_read.open("data_remote_read_read.csv");
    ofstream fout_poll;
    fout_poll.open("data_remote_read_poll.csv");

    vector<long long int> read_times(num_reruns), poll_times(num_reruns);

    // input number of nodes and the local node id
    int num_nodes, node_rank;
    cin >> num_nodes;
    cin >> node_rank;

    // input the ip addresses
    map<uint32_t, string> ip_addrs;
    for(int i = 0; i < num_nodes; ++i) {
        cin >> ip_addrs[i];
    }

    // create all tcp connections and initialize global rdma resources
    initialize(node_rank, ip_addrs);

    for(long long int size = 10; size < max_size; ++size) {
        // create buffer for write and read
        char *write_buf, *read_buf;
        write_buf = (char *)malloc(size);
        read_buf = (char *)malloc(size);

        int r_index = num_nodes - 1 - node_rank;
        resources *res = new resources(r_index, write_buf, read_buf, size, size);

        // write to the write buffer
        for(int i = 0; i < size - 1; ++i) {
            write_buf[i] = 'a';
        }
        write_buf[size - 1] = 0;

        // start the timing experiment
        struct timespec start_time;
        struct timespec end_time;
        long long int nanoseconds_elapsed;

        struct timespec read_start_time;
        struct timespec read_end_time;
        struct timespec poll_start_time;
        struct timespec poll_end_time;

        clock_gettime(CLOCK_REALTIME, &start_time);

        for(int i = 0; i < num_reruns; ++i) {
            clock_gettime(CLOCK_REALTIME, &read_start_time);
            // read the entire buffer
            res->post_remote_read(size);
            clock_gettime(CLOCK_REALTIME, &read_end_time);
            clock_gettime(CLOCK_REALTIME, &poll_start_time);
            // poll for completion
            verbs_poll_completion();
            clock_gettime(CLOCK_REALTIME, &poll_end_time);

            read_times[i] = (read_end_time.tv_sec - read_start_time.tv_sec) * 1000000000 + (read_end_time.tv_nsec - read_start_time.tv_nsec);
            poll_times[i] = (poll_end_time.tv_sec - poll_start_time.tv_sec) * 1000000000 + (poll_end_time.tv_nsec - poll_start_time.tv_nsec);
        }

        clock_gettime(CLOCK_REALTIME, &end_time);
        nanoseconds_elapsed = (end_time.tv_sec - start_time.tv_sec) * 1000000000 + (end_time.tv_nsec - start_time.tv_nsec);

        fout << size << ", " << (nanoseconds_elapsed + 0.0) / (1000 * num_reruns) << endl;

        // Gb is in bits, not bytes, B/ns = 8b/ns = 8Gb/s
        fout_bw << size << ", " << (8 * size * num_reruns + 0.0) / nanoseconds_elapsed << endl;

        // cout << "Buffer read is : " << read_buf << endl;

        // find out the average time spent in read and poll separately
        double read_sum, poll_sum;
        read_sum = poll_sum = 0.0;
        for(int i = 0; i < num_reruns; ++i) {
            read_sum += read_times[i];
            poll_sum += poll_times[i];
        }
        fout_read << size << ", " << read_sum / (1000.0 * num_reruns) << endl;
        fout_poll << size << ", " << poll_sum / (1000.0 * num_reruns) << endl;

        // sync to proceed to next iteration
        char temp_char;
        char tQ[2] = {'Q', 0};
        sock_sync_data(get_socket(r_index), 1, tQ, &temp_char);

        // free malloc()ed area
        free(write_buf);
        free(read_buf);
        // important to de-register memory
        delete(res);
    }

    verbs_destroy();
    fout.close();
    fout_bw.close();
    fout_read.close();
    fout_poll.close();
    return 0;
}
