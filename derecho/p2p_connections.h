#pragma once

#include <iostream>
#include <map>
#include <memory>
#include <experimental/optional>
#include <vector>

#include "sst/verbs.h"

namespace sst {
struct P2PParams {
    uint32_t my_node_id;
    std::vector<uint32_t> members;
    uint32_t window_size;
    uint64_t max_p2p_size;
};
class P2PConnections {
    const std::vector<uint32_t>& members;
    const std::uint32_t num_members;
    const uint32_t my_node_id;
    uint32_t my_index;
    const uint32_t window_size;
    const uint32_t max_msg_size;
    std::map<uint32_t, uint32_t> node_id_to_rank;
    // one element per member for P2P
    std::vector<std::unique_ptr<volatile char*>> incoming_p2p_buffers;
    std::vector<std::unique_ptr<volatile char*>> outgoing_p2p_buffers;
    std::vector<std::unique_ptr<resources_one_sided>> res_vec;
    std::vector<uint64_t> incoming_request_seq_nums, incoming_reply_seq_nums, outgoing_request_seq_nums, outgoing_reply_seq_nums;
    std::vector<bool> prev_mode;
    uint32_t num_puts = 0;
public:
  P2PConnections(const P2PParams params);
  P2PConnections(const P2PConnections&& old_connections, const std::vector<uint32_t> new_members);
  uint32_t get_node_rank(uint32_t node_id);
  uint64_t get_max_p2p_size();
  volatile char* probe(uint32_t rank);
  std::experimental::optional<std::pair<uint32_t, volatile char*>> probe_all();
  volatile char* get_sendbuffer_ptr(uint32_t rank, bool reply = false);
  void send(uint32_t rank);
  
};
}
