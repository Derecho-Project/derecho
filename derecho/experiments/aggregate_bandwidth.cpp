#include <fstream>

#include "aggregate_bandwidth.h"
#include "../sst/sst.h"

double aggregate_bandwidth(std::vector<uint32_t> members, uint32_t node_rank,
                           double bw) {
    sst::SST<Result, sst::Mode::Writes> *sst =
        new sst::SST<Result, sst::Mode::Writes>(members, node_rank);
    (*sst)[node_rank].bw = bw;
    sst->put();
    sst->sync_with_members();
    double total_bw = 0.0;
    unsigned int num_nodes = members.size();
    for(unsigned int i = 0; i < num_nodes; ++i) {
        total_bw += (*sst)[i].bw;
    }
    return total_bw / num_nodes;
}
