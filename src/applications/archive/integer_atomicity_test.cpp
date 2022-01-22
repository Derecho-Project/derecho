#include <climits>
#include <fstream>
#include <iostream>
#include <map>
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
    fout.open("data_integer_atomicity_test.csv");

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

    int size = 2;
    int num_reruns = 1000000;

    if(node_rank == 0) {
        short int a = 0;
        short int b = SHRT_MAX;
        // create buffer for write and read
        uint8_t *write_buf, *read_buf;
        write_buf = (uint8_t *)&a;
        read_buf = (uint8_t *)malloc(4);

        int r_index = num_nodes - 1 - node_rank;
        resources *res = new resources(r_index, write_buf, read_buf, size, 4);

        while(true) {
            a = b - a;
        }

        free(read_buf);
        // important to de-register memory
        delete(res);
    }

    else {
        int b;
        // create buffer for write and read
        uint8_t *write_buf, *read_buf;
        write_buf = (uint8_t *)malloc(4);
        read_buf = (uint8_t *)&b;
        ;

        int r_index = num_nodes - 1 - node_rank;
        resources *res = new resources(r_index, write_buf, read_buf, 4, size);

        for(int i = 0; i < num_reruns; ++i) {
            // index is 0, meaning that we start from the beginning and read the entire buffer
            res->post_remote_read(size);
            // poll for completion
            verbs_poll_completion();
            fout << b << endl;
        }

        // free malloc()ed area
        free(write_buf);
        // important to de-register memory
        delete(res);
    }

    verbs_destroy();
    fout.close();
    return 0;
}
