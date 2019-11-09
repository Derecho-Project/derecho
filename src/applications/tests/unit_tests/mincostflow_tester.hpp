#pragma once

#ifndef DERECHO_MINCOSTFLOW_TESTER_HPP
#define DERECHO_MINCOSTFLOW_TESTER_HPP

#include <derecho/mincostflow/mincostflow.hpp>

using namespace mincostflow;
namespace mincostflow {
std::unordered_set<Shard> new_assignment(std::unordered_set<Shard>& shards,
        std::unordered_map<node_id_t, derecho::fcs_id_t>& node_to_fcs,
        std::unordered_map<derecho::fcs_id_t, std::unordered_set<node_id_t>>& fcs_to_nodes);
std::unordered_map<node_id_t, derecho::fcs_id_t> node_to_fcs_from_fcs_to_nodes(
        const std::unordered_map<derecho::fcs_id_t, std::unordered_set<node_id_t>>& fcs_to_nodes);
void print(std::unordered_set<Shard>& shards,
        const std::unordered_map<node_id_t, derecho::fcs_id_t>& node_to_fcs);
void print_before_after(std::unordered_set<Shard>& shards,
        std::unordered_map<derecho::fcs_id_t, std::unordered_set<node_id_t>>& fcs_to_nodes);
}

#endif //DERECHO_MINCOSTFLOW_TESTER_HPP
