#pragma once

#include <memory>
#include <vector>

#include "node/node_collection.hpp"
#include "smc/smc_group.hpp"

namespace p2p {
class P2PConnections {
node::NodeCollection members;
std::vector<std::unique_ptr<smc::SMCGroup>> connections;
size_t max_payload_size;
size_t window_size;
msg::recv_upcall_t receive_upcall;

public:
  P2PConnections(const node::NodeCollection& members, size_t max_payload_size, size_t window_size, const msg::recv_upcall_t& receive_upcall);
  P2PConnections(P2PConnections&& oldP2PConnections, const node::NodeCollection& members);
};
}  // namespace p2p
