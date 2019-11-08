#include <derecho/sst/sst.hpp>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "initialize.h"

using std::vector;

using namespace sst;

class mySST : public SST<mySST> {
public:
    mySST(const vector<uint32_t>& members, uint32_t node_id)
            : SST<mySST>(this, SSTParams{members, node_id}) {
        SSTInit(counter);
    }
    SSTField<int> counter;
};

int main(int argc, char* argv[]) {
    if(argc < 3) {
        std::cout << "Usage: " << argv[0] << " <num_nodes> <kind of put (0 - put&sinc, 1 - put_with_completion)>" << std::endl;
        exit(1);
    }
    const uint32_t num_nodes = std::atoi(argv[1]);
    const uint32_t option = std::atoi(argv[2]);
    const uint32_t node_id = derecho::getConfUInt32(CONF_DERECHO_LOCAL_ID);
    const std::map<uint32_t, std::pair<ip_addr_t, uint16_t>> ip_addrs_and_ports = initialize(num_nodes);

    // initialize the rdma resources
#ifdef USE_VERBS_API
    verbs_initialize(ip_addrs_and_ports, node_id);
#else
    lf_initialize(ip_addrs_and_ports, node_id);
#endif

    vector<uint32_t> members(num_nodes);
    for(uint i = 0; i < num_nodes; ++i) {
        members[i] = i;
    }

    // create the SST
    mySST sst(members, node_id);
    uint32_t my_rank = sst.get_local_index();

    // initialize my own counter
    sst.counter[my_rank] = 0;

    // push the value to others
    if(option == 0) {
        sst.put();
        sst.sync_with_members();

    } else if(option == 1) {
        sst.put_with_completion();
    } else {
        std::cout << "Invalid option" << std::endl;
        std::cout << "Usage: " << argv[0] << " <num_nodes> <kind of put (0 - put&sinc, 1 - put_with_completion)>" << std::endl;
        exit(1);
    }

    // print out the values after the push
    for(uint i = 0; i < num_nodes; ++i) {
        std::cout << "sst.counter[" << i << "] = " << sst.counter[i] << std::endl;
    }
    
    return 0;
}