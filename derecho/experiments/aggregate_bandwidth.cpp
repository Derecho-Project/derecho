#include <fstream>

#include "aggregate_bandwidth.h"

double aggregate_bandwidth(std::vector<uint32_t> members, uint32_t node_rank,
                           double bw) {
    ResultSST *sst = new ResultSST(sst::SSTParams(members, node_rank));
    sst->bw[node_rank] = bw;
    sst->put();
    sst->sync_with_members();
    double total_bw = 0.0;
    unsigned int num_nodes = members.size();
    for(unsigned int i = 0; i < num_nodes; ++i) {
        total_bw += sst->bw[i];
    }
    return total_bw / num_nodes;
}
