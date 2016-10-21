#include "initialize.h"
#include "../sst/sst.h"
#include "../rdmc/util.h"
#include "../rdmc/message.h"
#include "../rdmc/verbs_helper.h"
#include "../rdmc/rdmc.h"
#include "../rdmc/microbenchmarks.h"
#include "../rdmc/group_send.h"

std::map<uint32_t, std::string> initialize (uint32_t &node_rank, uint32_t &num_nodes) {
    map<uint32_t, std::string> node_addresses;

    query_addresses(node_addresses, node_rank);
    num_nodes = node_addresses.size();

    // initialize RDMA resources, input number of nodes, node rank and ip addresses and create TCP connections
    if (!rdmc::initialize(node_addresses, node_rank)) {
        return node_addresses;
    }


    // initialize the rdma resources
    sst::verbs_initialize(node_addresses, node_rank);

    return node_addresses;
}
