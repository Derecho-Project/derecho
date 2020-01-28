#pragma once

#include <derecho/sst/sst.hpp>
#include <vector>

class ResultSST : public sst::SST<ResultSST> {
public:
    sst::SSTField<double> bw;
    ResultSST(const sst::SSTParams& params)
            : SST<ResultSST>(this, params) {
        SSTInit(bw);
    }
};
double aggregate_bandwidth(std::vector<uint32_t> members, uint32_t node_rank,
                           double bw);
double aggregate_bandwidth_tcp(std::string leader_ip, bool is_leader, uint32_t node_id, uint32_t num_members,
                               double bw);
