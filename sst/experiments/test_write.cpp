#include <iostream>
#include <map>
#include <thread>

#include "sst/poll_utils.h"
#ifdef USE_VERBS_API
#include "sst/verbs.h"
#else
#include "sst/lf.h"
#endif

using namespace std;
using namespace sst;

int main() {
    // input number of nodes and the local node id
    int num_nodes, node_rank;
    cin >> node_rank;
    cin >> num_nodes;

    // input the ip addresses
    map<uint32_t, string> ip_addrs;
    for(int i = 0; i < num_nodes; ++i) {
        cin >> ip_addrs[i];
    }

    // create all tcp connections and initialize global rdma resources
#ifdef USE_VERBS_API
    verbs_initialize(ip_addrs, node_rank);
#else
    lf_initialize(ip_addrs, node_rank);
#endif
    // create read and write buffers
    char *write_buf = (char *)malloc(10);
    char *read_buf = (char *)malloc(10);

    // write message (in a way that distinguishes nodes)
    for(int i = 0; i < 10; ++i) {
        write_buf[i] = '0' + i + node_rank % 10;
    }
    write_buf[9] = 0;

    cout << "write buffer is " << write_buf << endl;

    int r_index = num_nodes - 1 - node_rank;

    // create the rdma struct for exchanging data
    resources *res = new resources(r_index, read_buf, write_buf, 10, 10, node_rank < r_index);

    const auto tid = std::this_thread::get_id();
    // get id first
    uint32_t id = util::polling_data.get_index(tid);

    // remotely write data from the write_buf
    res->post_remote_write(id, 10);
    // poll for completion
    util::polling_data.get_completion_entry(tid);

    sync(r_index);

    cout << "Buffer written by remote side is : " << read_buf << endl;

    for(int i = 0; i < 10; ++i) {
        write_buf[i] = '0' + i + node_rank % 10;
    }
    write_buf[9] = 0;

    cout << "write buffer is " << write_buf << endl;

    sync(r_index);
    cout << "Buffer written by remote side is : " << read_buf << endl;

    // // destroy resources
    // delete(res);

    // // destroy global resources
    // verbs_destroy();

    return 0;
}
