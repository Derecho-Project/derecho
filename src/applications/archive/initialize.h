#pragma once
#include <derecho/tcp/tcp.hpp>
#include <derecho/core/derecho.hpp>

#include <map>

std::map<uint32_t, std::pair<ip_addr_t, uint16_t>> initialize(const uint32_t num_nodes);
