#include <iostream>

#include "sst/poll_utils.h"
#include "sst/verbs.h"
#include "tcp/tcp.h"

using std::cin;
using std::cout;
using std::endl;
using std::map;
using std::string;

using namespace sst;
using namespace tcp;

void initialize(int node_rank, const map<uint32_t, string> &ip_addrs) {
    // initialize the rdma resources
    verbs_initialize(ip_addrs, node_rank);
}

int main() {
    srand(time(NULL));
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
    initialize(node_rank, ip_addrs);

    int a;
    volatile int b;
    a = b = 0;
    // create read and write buffers
    char *write_buf = (char *)&a;
    char *read_buf = (char *)&b;

    int r_index = num_nodes - 1 - node_rank;

    // create the rdma struct for exchanging data
    resources_two_sided *res = new resources_two_sided(r_index, read_buf, write_buf, sizeof(int), sizeof(int));

    const auto tid = std::this_thread::get_id();
    // get id first
    uint32_t id = util::polling_data.get_index(tid);

    util::polling_data.set_waiting(tid);

    if(node_rank == 0) {
        // wait for random time
        volatile long long int wait_time = (long long int)5e5;
        for(long long int i = 0; i < wait_time; ++i) {
        }
        cout << "Wait finished" << endl;

        a = 1;
        res->post_two_sided_send(id, sizeof(int));
        res->post_two_sided_receive(id, sizeof(int));
        cout << "Receive buffer posted" << endl;
        while(b == 0) {
        }
    }

    else {
        res->post_two_sided_receive(id, sizeof(int));
        cout << "Receive buffer posted" << endl;
        while(b == 0) {
        }
        a = 1;
        cout << "Sending" << endl;
        res->post_two_sided_send(id, sizeof(int));
    }

    sync(r_index);
    return 0;
}
