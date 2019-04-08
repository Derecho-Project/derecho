#ifndef AGGREGATE_BANDWIDTH_H
#define AGGREGATE_BANDWIDTH_H

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
#endif
