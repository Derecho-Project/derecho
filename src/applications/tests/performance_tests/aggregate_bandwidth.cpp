#include <fstream>

#include <derecho/tcp/tcp.hpp>

#include "aggregate_bandwidth.hpp"

double aggregate_bandwidth(std::vector<uint32_t> members, uint32_t node_id,
                           double bw) {
    ResultSST sst(sst::SSTParams(members, node_id));
    sst.bw[sst.get_local_index()] = bw;
    sst.put();
    sst.sync_with_members();
    double total_bw = 0.0;
    unsigned int num_nodes = members.size();
    for(unsigned int i = 0; i < num_nodes; ++i) {
        total_bw += sst.bw[i];
    }
    return total_bw / num_nodes;
}

double aggregate_bandwidth_tcp(std::string leader_ip, bool is_leader, uint32_t node_id, uint32_t num_members,
                               double bw) {
    uint16_t port = 9826;
    double total_bw = bw;
    if (is_leader) {
        tcp::connection_listener c_l(port);
        for(uint i = 0; i < num_members - 1; ++i) {
            tcp::socket s = c_l.accept();
	    double recvd_bw;
	    s.read(recvd_bw);
	    total_bw += recvd_bw;
        }
    }
    else {
        std::this_thread::sleep_for(std::chrono::milliseconds(node_id));
        tcp::socket s(leader_ip, port);
    	s.write(bw);
    }

    return total_bw / num_members;
}
