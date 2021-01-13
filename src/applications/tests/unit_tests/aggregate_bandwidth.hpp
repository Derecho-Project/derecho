#pragma once

#include <derecho/sst/sst.hpp>
#include <vector>

class OneResultSST : public sst::SST<OneResultSST> {
public:
    sst::SSTField<double> bw;
    OneResultSST(const sst::SSTParams& params)
            : SST<OneResultSST>(this, params) {
        SSTInit(bw);
    }
};

class TwoResultSST : public sst::SST<TwoResultSST> {
public:
    sst::SSTField<double> bw1;
    sst::SSTField<double> bw2;
    TwoResultSST(const sst::SSTParams& params)
            : SST<TwoResultSST>(this, params) {
        SSTInit(bw1, bw2);
    }
};

double aggregate_bandwidth(std::vector<uint32_t> members, uint32_t node_rank,
                           double bw);

std::pair<double, double> aggregate_bandwidth(std::vector<uint32_t> members, uint32_t node_rank,
                                              std::pair<double, double> bw);
