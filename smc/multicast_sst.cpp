#include "multicast_sst.hpp"

namespace smc {

MulticastSST::MulticastSST(const node::NodeCollectionWithSenders& members_with_senders_info, size_t slots_size)
        : sst::SST<MulticastSST>(this, members_with_senders_info),
          slots(slots_size),
          received_nums(members_with_senders_info.num_senders) {
    initialize(slots, received_nums);

    // just initialize received_num, no need to zero slots
    for(auto row = 0u; row < members_with_senders_info.num_nodes; ++row) {
        for(auto index = 0u; index < members_with_senders_info.num_senders; ++index) {
            received_nums[row][index] = -1;
        }
    }
    sync_with_members();
}
}  // namespace smc
