#include <derecho/sst/sst.hpp>
#ifdef USE_VERBS_API
    #include <derecho/sst/detail/verbs.hpp>
#else
    #include <derecho/sst/detail/lf.hpp>
#endif
#include <spdlog/spdlog.h>

using std::cin;
using std::cout;
using std::endl;
using std::map;
using std::vector;

class mySST : public sst::SST<mySST> {
public:
    mySST(const vector<uint32_t>& _members, uint32_t my_rank) : SST(this, sst::SSTParams{_members, my_rank}) {
        SSTInit(a);
    }
    sst::SSTField<int> a;
};

int main() {
    spdlog::set_level(spdlog::level::trace);
    // input number of nodes and the local node rank
    std::cout << "Enter my_rank and num_nodes" << std::endl;
    uint32_t my_rank, num_nodes;
    cin >> my_rank >> num_nodes;

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
    sst::verbs_initialize(ip_addrs_and_ports, {}, my_rank);
#else
    sst::lf_initialize(ip_addrs_and_ports, {}, my_rank);
#endif

    vector<uint32_t> members(num_nodes);
    for(uint i = 0; i < num_nodes; ++i) {
        members[i] = i;
    }

    mySST sst(members, my_rank);
    int b = 5 + my_rank;
    sst.a(my_rank, b);
    sst.put();
    sst::sync(1 - my_rank);
    int n;
    cin >> n;
    for(uint i = 0; i < num_nodes; ++i) {
        cout << sst.a(i) << endl;
    }
    while(true) {
    }
}
