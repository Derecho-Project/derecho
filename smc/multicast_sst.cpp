#include "multicast_sst.hpp"

MulticastSST::MulticastSST(const node::NodeCollectionWithSenders& members_with_senders_info, size_t slot_size)
        : window_size(window_size),
          max_msg_size(max_msg_size),
          slots(window_size * max_msg_size),
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

bool MulticastSST::send(size_t msg_size, const std::function<void(char* buf)>& msg_generator) {
    std::lock_guard<std::mutex> lock(msg_send_mutex);
    
}
