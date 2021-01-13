#pragma once
#include <derecho/tcp/tcp.hpp>
#include <derecho/core/derecho.hpp>

#include <map>
#include <tuple>

/**
 * Reads the Derecho config file to determine the leader node's IP and ports,
 * then communicates with the leader to find out which other nodes are
 * participating in the test. This performs the same start-up sequence as
 * a Derecho group waiting for an initial view with a minumum number of nodes,
 * but without using Derecho's group management system. This makes it useful
 * for testing smaller sub-components of Derecho in isolation, like SST.
 * @param num_nodes The number of nodes that are expected to participate in the
 * test. The function will block until this many nodes have contacted the leader.
 * @return A map from node IDs to (IP address, SST port) pairs, with one entry
 * for each node participating in the test.
 */
std::map<uint32_t, std::pair<ip_addr_t, uint16_t>> initialize(const uint32_t num_nodes);
