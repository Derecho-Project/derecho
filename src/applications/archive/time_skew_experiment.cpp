#include <iostream>

#include "sst/tcp.h"
#include "time_skew.h"

using namespace std;
using namespace sst;

int main() {
    // input number of nodes and the local node id
    uint32_t node_id, num_nodes;
    cin >> node_id >> num_nodes;

    // input the ip addresses
    map<uint32_t, string> ip_addrs;
    for(unsigned int i = 0; i < num_nodes; ++i) {
        cin >> ip_addrs[i];
    }

    // initialize tcp connections
    tcp::tcp_initialize(node_id, ip_addrs);

    // initialize the rdma resources
    verbs_initialize();

    assert(num_nodes == 2);
    SST<TimeRow> sst({0, 1}, node_id);
    sst[0].time_in_nanoseconds = -1;
    sst[1].time_in_nanoseconds = -1;
    sst.sync_with_members();

    if(node_id == 0) {
        cout << server(sst, 0, 1) << endl;
        sst.sync_with_members();
        client(sst, 0, 1);
        sst.sync_with_members();
    } else {
        client(sst, 1, 0);
        sst.sync_with_members();
        cout << server(sst, 1, 0) << endl;
        sst.sync_with_members();
    }
    return 0;
}
