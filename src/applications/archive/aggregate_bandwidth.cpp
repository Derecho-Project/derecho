#include <fstream>

//Debug
#include <iostream>

#include "aggregate_bandwidth.hpp"

double aggregate_bandwidth(std::vector<uint32_t> members, uint32_t node_rank,
                           double bw) {
    ResultSST sst(sst::SSTParams(members, node_rank));
    sst.bw[node_rank] = bw;
    sst.put_with_completion();
    sst.sync_with_members();
    double total_bw = 0.0;
    unsigned int num_nodes = members.size();

    //Debug
    std::cout << members.size() << " " << node_rank << " " << bw << std::endl;

    for(unsigned int i = 0; i < num_nodes; ++i) {
        total_bw += sst.bw[i];
        
        //Debug
        std::cout << "sst.bw[" << i << "] = " << sst.bw[i] << std::endl;

    }
    return total_bw / num_nodes;
}
