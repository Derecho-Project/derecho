#pragma once

#include <iostream>
#include <memory>
#include <vector>

#include "poll_utils.h"
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
    std::vector<std::unique_ptr<volatile char*>> incoming_p2p_buffers;
    std::vector<std::unique_ptr<volatile char*>> outgoing_p2p_buffers;
    std::vector<std::unique_ptr<resources_one_sided>> res_vec;
    std::vector<uint64_t> incoming_request_seq_nums, incoming_reply_seq_nums, outgoing_request_seq_nums, outgoing_reply_seq_nums;
    std::vector<bool> prev_mode;
    uint32_t num_puts = 0;
public:
  P2PConnections(const P2PParams& params);
  P2PConnections(P2PConnections&& old_connections, const P2PParams& params);
  volatile char* P2PConnections::probe(uint32_t rank);
  pair<uint32_t, volatile char*> P2PConnections::probe_all();
  volatile char* P2PConnections::get_sendbuffer_ptr(uint32_t rank, bool reply = false);
  void P2PConnections::send(uint32_t rank);
  
}
}
