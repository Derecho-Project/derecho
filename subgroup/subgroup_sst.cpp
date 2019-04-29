#include "subgroup_sst.hpp"

namespace subgroup {
SubgroupSST::SubgroupSST(const node::NodeCollection& members)
        : sst::SST<SubgroupSST>(this, members),
          received_nums(members.num_nodes),
          min_accepted_nums(members.num_nodes) {
    initialize(received_nums, seq_num, delivered_num,
               persisted_num, min_accepted_nums,
               ragged_edge_computed);

    for(auto row = 0u; row < members.num_nodes; ++row) {
        for(auto index = 0u; index < members.num_nodes; ++index) {
            received_nums[row][index] = -1;
	    min_accepted_nums[row][index] = -1;
        }
	seq_num[row] = -1;
	delivered_num[row] = -1;
	persisted_num[row] = -1;
	ragged_edge_computed[row] = false;
    }
    sync_with_members();
}
}  // namespace subgroup
