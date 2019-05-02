#pragma once

#include <map>

#include "node/node.hpp"
#include "tcp/tcp.hpp"

std::map<node::node_id_t, std::pair<tcp::ip_addr_t, tcp::port_t>> initialize(const uint32_t num_nodes);
