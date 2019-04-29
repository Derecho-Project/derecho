#include "multicast_sst.hpp"

MulticastSST::MulticastSST(const node::nodeCollection& members, size_t slot_size)
        : window_size(window_size),
          max_msg_size(max_msg_size),
          slots(window_size * max_msg_size),
          received_num(members.num_nodes) {
    initialize(slots, received_num);

    // just initialize received_num, no need to zero slots
    for(auto row = 0u; row < members.num_nodes; ++row) {
        for(auto index = 0u; index < members.num_nodes; ++index) {
            received_num[row][index] = -1;
        }
    }
    sync_with_members();
}

bool MulticastSST::send(size_t msg_size, const std::function<void(char* buf)>& msg_generator) {
    std::lock_guard<std::mutex> lock(msg_send_mutex);
    
}
