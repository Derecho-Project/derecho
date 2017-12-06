#pragma once

#include <iostream>
#include <memory>
#include <vector>

#include "sst/verbs.h"

namespace sst {
class P2PConnections {
    const std::vector<uint32_t>& members;
    const std::uint32_t num_members;
    const uint32_t my_node_id;
    uint32_t my_index;
    const uint32_t window_size;
    const uint32_t max_msg_size;
    // one element per member for P2P
    std::vector<std::unique_ptr<std::string>> outgoing_p2p_buffers;
    std::vector<std::unique_ptr<std::string>> incoming_p2p_buffers;
    std::vector<std::unique_ptr<resources_one_sided>> res_vec;

public:
    P2PConnections(const P2PParams& params)
            : members(params.members),
              num_members(members.size()),
              my_node_id(params.my_node_id),
              window_size(params.window_size),
	      max_msg_size(params.max_p2p_size + sizeof(uint64_t)),
              p2p_buffers(2 * num_members),
              res_vec(num_members) {
        //Figure out my SST index
        my_index = -1;
        for(uint32_t i = 0; i < num_members; ++i) {
            if(members[i] == my_node_id) {
                my_index = i;
                break;
            }
        }
        assert(my_index != -1);

        for(uint i = 0; i < num_members; ++i) {
            if(i == my_index) {
                continue;
            }
            outgoing_p2p_buffers[i] = std::make_unique<std::string>(2 * max_msg_size * window_size, 0);
            incoming_p2p_buffers[i] = std::make_unique<std::string>(2 * max_msg_size * window_size, 0);
            res_vec[i] = std::make_unique<resources_one_sided>(i, incoming_p2p_buffers[i].get(), outgoing_p2p_buffers[i].get(), 2 * max_msg_size * window_size, 2 * max_msg_size * window_size);
        }
    }

  
}
}
