#include <fstream>
#include <iostream>
#include <map>
#include <stdio.h>
#include <string.h>
#include <vector>

#include "sst/tcp.h"
#include "sst/verbs.h"

using std::cin;
using std::cout;
using std::endl;
using std::map;
using std::ofstream;
using std::string;
using std::vector;

using namespace sst;
using namespace sst::tcp;

void initialize(int node_rank, const map<uint32_t, string> &ip_addrs) {
    // initialize tcp connections
    tcp_initialize(node_rank, ip_addrs);

    // initialize the rdma resources
    verbs_initialize();
}

int main() {
    ofstream fout;
    fout.open("data_memcpy_atomicity_test.csv");

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

    int size = 2048;
    int num_reruns = 10000;

    if(node_rank == 0) {
        // create buffer for write and read
        char *write_buf, *read_buf;
        write_buf = (char *)malloc(size);
        read_buf = (char *)malloc(4);

        char *a_str, *b_str;
        a_str = (char *)malloc(size);
        b_str = (char *)malloc(size);

        for(int i = 0; i < size - 1; ++i) {
            a_str[i] = 'a';
            b_str[i] = 'b';
        }
        a_str[size - 1] = 0;
        b_str[size - 1] = 0;

        int r_index = num_nodes - 1 - node_rank;
        resources *res = new resources(r_index, write_buf, read_buf, size, 4);

        char *ptr = a_str;

        while(true) {
            memcpy(write_buf, ptr, size);
            if(ptr == a_str) {
                ptr = b_str;
            } else {
                ptr = a_str;
            }
        }

        // free malloc()ed area
        free(write_buf);
        free(read_buf);
        // important to de-register memory
        delete(res);
    }

    else {
        // create buffer for write and read
        char *write_buf, *read_buf;
        write_buf = (char *)malloc(4);
        read_buf = (char *)malloc(size);

        int r_index = num_nodes - 1 - node_rank;
        resources *res = new resources(r_index, write_buf, read_buf, 4, size);

        for(int i = 0; i < num_reruns; ++i) {
            // index is 0, meaning that we start from the beginning and read the entire buffer
            res->post_remote_read(size);
            // poll for completion
            verbs_poll_completion();
            fout << read_buf << endl;
        }

        // free malloc()ed area
        free(write_buf);
        free(read_buf);
        // important to de-register memory
        delete(res);
    }

    verbs_destroy();
    fout.close();
    return 0;
}
