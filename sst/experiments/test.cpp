#include <iostream>
#include <map>

#include "sst/verbs.h"

using namespace std;
using namespace sst;

void initialize(int node_rank, const map<uint32_t, string> &ip_addrs) {
    // initialize the rdma resources
    verbs_initialize(node_rank, ip_addrs);
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

    // create read and write buffers
    char *write_buf = (char *)malloc(10);
    char *read_buf = (char *)malloc(10);

    // write message (in a way that distinguishes nodes)
    for(int i = 0; i < 9; ++i) {
        write_buf[i] = '0' + node_rank % 10;
    }
    write_buf[9] = 0;

    cout << "write buffer is " << write_buf << endl;

    int r_index = num_nodes - 1 - node_rank;

    // create the rdma struct for exchanging data
    resources_one_sided *res = new resources_one_sided(r_index, write_buf, read_buf, 10, 10);

    // remotely read data into the read_buf
    res->post_remote_read(10);
    // post_remote_read (0, 10, res->read_buf, res->read_mr, res->remote_props, res->qp);
    // poll for completion
    verbs_poll_completion();
    cout << "Buffer read is : " << read_buf << endl;

    sync(r_index);

    // destroy resources
    delete(res);

    // destroy global resources
    verbs_destroy();

    return 0;
}
