#include "subgroupSST.hpp"

namespace subgroup {
SubgroupSST::SubgroupSST(node::NodeCollection members)
        : sst::SST<SubgroupSST>(this, members),
          received_num(members.num_nodes),
          min_accepted_num(members.num_nodes) {
    initialize(received_num, seq_num, delivered_num, persisted_num, wedged, min_accepted_num, ragged_edge_computed);

    for(auto node = 0u; node < members.num_nodes; ++node) {
        for(auto index = 0u; index < received_num.size(); ++index) {
            received_num[node][index] = -1;
        }
    }
    std::fill(seq_num.begin(), seq_num.end(), -1);
    std::fill(delivered_num.begin(), delivered_num.end(), -1);
    std::fill(persisted_num.begin(), persisted_num.end(), -1);
}
}  // namespace subgroup
