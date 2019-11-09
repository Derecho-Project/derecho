#include <mincostflow_tester.hpp>
#include <iostream>

using namespace mincostflow;

int main(int argc, char* argv[]) {
    std::unordered_set<Shard> shards;
    std::unordered_map<derecho::fcs_id_t, std::unordered_set<node_id_t>> fcs_to_nodes;
    // subgroup ID is always 0
    // shard ID starts from 0
    // node ID starts from 100
    // fcs ID starts from 200

    //all shards take from unassigned
    fcs_to_nodes[200] = {100, 101, 102};
    fcs_to_nodes[201] = {103, 104, 105};
    fcs_to_nodes[202] = {106};
    shards.insert({{0, 0}, 0, {100, 101, 102, 103, 104, 105, 106}}); //unassigned
    shards.insert({{0, 1}, 2, {}});
    shards.insert({{0, 2}, 3, {}});
    shards.insert({{0, 3}, 2, {}});
    print_before_after(shards, fcs_to_nodes);
    shards.clear();
    fcs_to_nodes.clear();

    //shard 1 takes from 2, shard 2 takes from unassigned
    fcs_to_nodes[200] = {100, 101};
    fcs_to_nodes[201] = {102, 103};
    fcs_to_nodes[202] = {104};
    shards.insert({{0, 0}, 0, {100}}); //unassigned
    shards.insert({{0, 1}, 3, {101, 102}});
    shards.insert({{0, 2}, 2, {103, 104}});
    print_before_after(shards, fcs_to_nodes);
    shards.clear();
    fcs_to_nodes.clear();
}

namespace mincostflow {

std::unordered_set<Shard> new_assignment(std::unordered_set<Shard>& shards,
    std::unordered_map<node_id_t, derecho::fcs_id_t>& node_to_fcs,
    std::unordered_map<derecho::fcs_id_t, std::unordered_set<node_id_t>>& fcs_to_nodes) {
    FcsAllocator fcs_allocator = {shards, node_to_fcs, fcs_to_nodes};
    return fcs_allocator.allocate();
}
std::unordered_map<node_id_t, derecho::fcs_id_t> node_to_fcs_from_fcs_to_nodes(
        const std::unordered_map<derecho::fcs_id_t, std::unordered_set<node_id_t>>& fcs_to_nodes) {
    std::unordered_map<node_id_t, derecho::fcs_id_t> node_to_fcs;
    for (const auto& [fcs, nodes] : fcs_to_nodes) {
        for (node_id_t node : nodes) {
            node_to_fcs[node] = fcs;
        }
    }
    return node_to_fcs;
}
void print(std::unordered_set<Shard>& shards,
        const std::unordered_map<node_id_t, derecho::fcs_id_t>& node_to_fcs) {
    for (const Shard& shard : shards) {
        std::cout << shard.shard_id.second;
        if (shard.members.size() < shard.min_fcs) {
            std::cout << ", UNSATISFIED";
        }
        std::cout << ": {" << std::endl;

        for (node_id_t node : shard.members) {
            std::cout << '\t' << node << ", fcs: " << node_to_fcs.at(node) << std::endl;
        }
        std::cout << "}" << std::endl;
    }
}
void print_before_after(std::unordered_set<Shard>& shards,
    std::unordered_map<derecho::fcs_id_t, std::unordered_set<node_id_t>>& fcs_to_nodes) {
    std::unordered_map<node_id_t, derecho::fcs_id_t> node_to_fcs =
            node_to_fcs_from_fcs_to_nodes(fcs_to_nodes);
    std::cout << "Before:" << std::endl;
    print(shards, node_to_fcs);
    std::cout << "After:" << std::endl;
    std::unordered_set<Shard> new_shards = new_assignment(shards, node_to_fcs, fcs_to_nodes);
    print(new_shards, node_to_fcs);
}

}