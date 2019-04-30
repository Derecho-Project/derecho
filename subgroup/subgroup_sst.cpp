#include "subgroup_sst.hpp"

namespace subgroup {
SubgroupSST::SubgroupSST(const node::NodeCollectionWithSenders& members_with_senders_info)
        : sst::SST<SubgroupSST>(this, members_with_senders_info),
          received_nums(members_with_senders_info.num_senders),
          min_accepted_nums(members_with_senders_info.num_senders) {
    initialize(received_nums, seq_num, delivered_num,
               persisted_num, min_accepted_nums,
               ragged_edge_computed);

    for(auto row = 0u; row < members_with_senders_info.num_nodes; ++row) {
        for(auto index = 0u; index < members_with_senders_info.num_senders; ++index) {
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
