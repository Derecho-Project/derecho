#include <iostream>

#include "../sst.h"
#include "../tcp.h"

using namespace sst;
using std::cin;
using std::cout;

struct TestRow {
    int b;
    int a;
};

int main() {
    // input number of nodes and the local node id
    uint32_t num_nodes, node_rank;
    cin >> node_rank >> num_nodes;

    assert((num_nodes == 5) && "This basic test is designed for only 5 nodes");

    // input the ip addresses
    std::map<uint32_t, std::string> ip_addrs;
    for(uint32_t i = 0; i < num_nodes; ++i) {
        cin >> ip_addrs[i];
    }

    // initialize tcp connections
    sst::tcp::tcp_initialize(node_rank, ip_addrs);

    // initialize the rdma resources
    verbs_initialize();

    // form a group with a subset of all the nodes
    vector<uint32_t> members(num_nodes);
    for(unsigned int i = 0; i < num_nodes; ++i) {
        members[i] = i;
    }

    // create a new shared state table with all the members
    SST<TestRow, Mode::Writes> sst(members, node_rank);

    sst[node_rank].b = 0;
    sst[node_rank].a = 0;
    sst.put();
    sst.sync_with_members();

    if(node_rank == 0) {
        sst[0].a = 7;
        sst[0].b = 9;
        sst.put({0, 1, 2},
                (char*)std::addressof(sst[0].a) - (char*)std::addressof(sst[0]),
                sizeof(sst[0].a));
    } else if(node_rank <= 2) {
        while(sst[0].a != 7) {
        }
    }

    sst.sync_with_members();
    cout << "Test completed" << endl;
    cout << "Value of b for node 0's row is: " << sst[0].b << endl;
    cout << "Value of a for node 0's row is: " << sst[0].a << endl;
}
