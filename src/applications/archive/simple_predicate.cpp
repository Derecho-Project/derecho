#include <functional>
#include <iostream>
#include <stdlib.h>

#include <derecho/sst/sst.hpp>
#ifdef USE_VERBS_API
#include <derecho/sst/detail/verbs.hpp>
#else
#include <derecho/sst/detail/lf.hpp>
#endif

using std::cin;
using std::cout;
using std::endl;
using std::map;
using std::vector;

class mySST : public sst::SST<mySST> {
public:
    mySST(const vector<uint32_t>& _members, uint32_t my_rank) : SST<mySST>(this, sst::SSTParams{_members, my_rank}) {
        SSTInit(a);
    }
    sst::SSTField<int> a;
};

int main() {
    using namespace sst;

    // input number of nodes and the local node rank
    std::cout << "Enter node_rank and num_nodes" << std::endl;
    uint32_t node_rank, num_nodes;
    cin >> node_rank >> num_nodes;

    std::cout << "Input the IP addresses" << std::endl;
    uint16_t port = 32567;
    // input the ip addresses
    map<uint32_t, std::pair<std::string, uint16_t>> ip_addrs_and_ports;
    for(uint i = 0; i < num_nodes; ++i) {
      std::string ip;
      cin >> ip;
      ip_addrs_and_ports[i] = {ip, port};
    }
    std::cout << "Using the default port value of " << port << std::endl;

    // initialize the rdma resources
#ifdef USE_VERBS_API
    sst::verbs_initialize(ip_addrs_and_ports, node_rank);
#else
    sst::lf_initialize(ip_addrs_and_ports,node_rank);
#endif

    vector<uint32_t> members(num_nodes);
    for(uint i = 0; i < num_nodes; ++i) {
        members[i] = i;
    }

    mySST sst(members, node_rank);
    sst.a(node_rank, 0);
    sst.put();
    // will add sync later
    cout << "First input" << endl;
    int n;
    cin >> n;

    auto f = [node_rank](const mySST& sst) {
        return sst.a[1 - node_rank] != 0;
    };

    if(node_rank == 0) {
        auto g = [node_rank](mySST& sst) {
            sst.a[node_rank] = 1;
            sst.put();
            cout << "Exiting" << endl;
            exit(0);
        };

        sst.predicates.insert(f, g);
    }

    else {
        auto g = [](mySST& sst) {
            cout << "Exiting" << endl;
            exit(0);
        };

        sst.predicates.insert(f, g);

        cout << "Second input" << endl;
        int k;
        cin >> k;
        sst.a(node_rank, 1);
        sst.put();
    }

    cout << "Waiting" << endl;
    while(true) {
    }
}
