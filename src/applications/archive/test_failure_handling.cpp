#include <chrono>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <map>

#include "sst/sst.h"
#include "sst/tcp.h"
//Since all SST instances are named sst, we can use this convenient hack
#define LOCAL sst.get_local_index()

using namespace sst;
using std::cin;
using std::cout;

struct TestRow {
    int a;
};

int main() {
    // input number of nodes and the local node id
    uint32_t num_nodes, node_rank;
    cin >> node_rank >> num_nodes;

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
    SST<TestRow, Mode::Writes> *sst = new SST<TestRow, Mode::Writes>(members, node_rank, [](uint32_t failed_node_rank) { std::cout << "Node " << failed_node_rank << " failed" << endl; });

    // the predicate - as simple as it can be
    auto f = [](const SST<TestRow, Mode::Writes> &sst) {
        return true;
    };

    // the trigger - calls put - required to detect failures
    auto g = [](SST<TestRow, Mode::Writes> &sst) {
        sst.put();
    };

    // insert the predicate and trigger
    sst->predicates.insert(f, g, PredicateType::RECURRENT);

    // exit after node_rank+1 seconds
    std::this_thread::sleep_for(std::chrono::seconds(10 * (node_rank + 1)));
}
