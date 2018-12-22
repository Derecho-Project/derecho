#include <fstream>

#include "aggregate_latency.h"

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
