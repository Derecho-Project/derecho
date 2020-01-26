#include <fstream>

#include <derecho/tcp/tcp.hpp>

#include "aggregate_latency.hpp"

std::pair<double, double> aggregate_latency(std::vector<uint32_t> members, uint32_t node_id,
			 double latency, double latency_std_dev) {
    ResultSST sst(sst::SSTParams(members, node_id));
    sst.latency[sst.get_local_index()] = latency;
    sst.latency_std_dev[sst.get_local_index()] = latency_std_dev;
    sst.put();
    sst.sync_with_members();
    double total_latency = 0.0;
    double total_latency_std_dev = 0.0;
    unsigned int num_nodes = members.size();
    uint count = 0;
    for(unsigned int i = 0; i < num_nodes; ++i) {
      if (sst.latency[i]) {
	total_latency += sst.latency[i];
	total_latency_std_dev += sst.latency_std_dev[i];
	count++;
      }
      
    }
    return {total_latency / count, total_latency_std_dev / count};
}


std::pair<double, double> aggregate_latency_tcp(std::string leader_ip, bool is_leader, uint32_t num_members,
                               double latency, double latency_std_dev) {
    uint16_t port = 9826;
    double total_latency = latency;
    double total_latency_std_dev = latency_std_dev;

    if (is_leader) {
        tcp::connection_listener c_l(port);
        for(uint i = 0; i < num_members - 1; ++i) {
            tcp::socket s = c_l.accept();
	          double recvd_latency;
	          s.read(recvd_latency);
            total_latency += recvd_latency;
            double recvd_latency_std_dev;
	          s.read(recvd_latency_std_dev);
            total_latency_std_dev += recvd_latency_std_dev;
        }
    }
    else {
        tcp::socket s(leader_ip, port);
	      s.write(latency);
        s.write(latency_std_dev);
    }

    return {total_latency / num_members, total_latency_std_dev / num_members};
}
