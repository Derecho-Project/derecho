#include <iostream>
#include <map>
#include <vector>

#include "sst/tcp.h"
#include "sst/verbs.h"

using namespace std;
using namespace sst;
using namespace sst::tcp;

void initialize(int node_rank, const map<uint32_t, string> &ip_addrs) {
    // initialize tcp connections
    tcp_initialize(node_rank, ip_addrs);

    // initialize the rdma resources
    verbs_initialize();
}

int main() {
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

    int size = 16;
    char *write_buf, *read_buf;
    write_buf = (char *)malloc(size);
    read_buf = (char *)malloc(size);

    int r_index = num_nodes - 1 - node_rank;
    resources *res = new resources(r_index, write_buf, read_buf, size, size);

    for(int i = 0; i < 10; ++i) {
        res->post_remote_read(size);
    }

    int a;
    cin >> a;
    for(int i = 0; i < 10; ++i) {
        verbs_poll_completion();
    }

    return 0;
}
