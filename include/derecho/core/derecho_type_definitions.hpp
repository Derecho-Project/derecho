#pragma once
/** Type alias for IP addresses, currently stored as strings. */
using ip_addr_t = std::string;
/** Type alias for Node IDs in a Derecho group. */
using node_id_t = uint32_t;
#define INVALID_NODE_ID (0xffffffff)
