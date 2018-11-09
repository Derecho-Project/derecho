
#include "rdmc/rdmc.h"
#include "sst/sst.h"

#include "initialize.h"

#include <cstdint>
#include <iostream>
#include <map>
#include <string>

using std::cin;
using std::cout;

std::map<uint32_t, std::string> initialize(uint32_t& node_rank, uint32_t& num_nodes) {
    std::map<uint32_t, std::string> node_addresses;

    rdmc::query_addresses(node_addresses, node_rank);
    num_nodes = node_addresses.size();

    // initialize RDMA resources, input number of nodes, node rank and ip addresses and create TCP connections
    bool success = rdmc::initialize(node_addresses, node_rank);
    if(!success) {
        exit(-1);
    }
    return node_addresses;
}

void query_node_info(ip_addr_t& leader_ip, uint16_t& leader_gms_port) {
    cout << "Please enter the leader node's IP address: ";
    cin >> leader_ip;
    cout << "Please enter the leader node's GMS port: ";
    cin >> leader_gms_port;
}
