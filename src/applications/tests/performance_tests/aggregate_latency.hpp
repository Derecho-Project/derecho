#pragma once

#include <derecho/sst/sst.hpp>
#include <vector>

class ResultSST : public sst::SST<ResultSST> {
public:
    sst::SSTField<double> latency;
    sst::SSTField<double> latency_std_dev;
    ResultSST(const sst::SSTParams& params)
            : SST<ResultSST>(this, params) {
        SSTInit(latency, latency_std_dev);
    }
};
std::pair<double, double> aggregate_latency(std::vector<uint32_t> members, uint32_t node_rank,
			 double latency, double latency_std_dev);

std::pair<double, double> aggregate_latency_tcp(std::string leader_ip, bool is_leader, uint32_t num_members,
             double latency, double latency_std_dev);