#pragma once

#include <derecho/sst/sst.hpp>
#include <vector>

class LatencyResultSST : public sst::SST<LatencyResultSST> {
public:
    sst::SSTField<double> latency;
    sst::SSTField<double> latency_std_dev;
    LatencyResultSST(const sst::SSTParams& params)
            : SST<LatencyResultSST>(this, params) {
        SSTInit(latency, latency_std_dev);
    }
};
std::pair<double, double> aggregate_latency(std::vector<uint32_t> members, uint32_t node_rank,
			 double latency, double latency_std_dev);
