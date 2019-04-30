#include "node_collection.hpp"
#include "exception/node_exceptions.hpp"

namespace node {
uint32_t get_my_rank(const std::vector<node_id_t>& nodes, const node_id_t my_id) {
    for(auto rank = 0u; rank < nodes.size(); ++rank) {
        if(nodes[rank] == my_id) {
            return rank;
        }
    }
    throw NodeIDNotFound("My id, which is " + std::to_string(my_id) + ", not found in the list of members");
}

std::vector<uint32_t> get_other_ranks(const uint32_t my_rank, const uint32_t num_nodes) {
    std::vector<uint32_t> other_ranks;
    for(auto rank = 0u; rank < my_rank; ++rank) {
        other_ranks.push_back(rank);
    }
    for(auto rank = my_rank + 1; rank < num_nodes; ++rank) {
        other_ranks.push_back(rank);
    }
    return other_ranks;
}

NodeCollection::NodeCollection(const std::vector<node_id_t>& nodes, const node_id_t my_id)
        : nodes(nodes),
          num_nodes(nodes.size()),
          my_id(my_id),
          my_rank(get_my_rank(nodes, my_id)),
          other_ranks(get_other_ranks(my_rank, num_nodes)) {
    for(uint32_t i = 0; i < num_nodes; ++i) {
        node_id_to_rank[nodes[i]] = i;
    }
}

uint32_t NodeCollection::get_rank_of(node_id_t node_id) const {
    try {
        return node_id_to_rank.at(my_id);
    } catch(std::out_of_range) {
        throw IDNotInMembers("ID " + std::to_string(node_id) + " not found in the list of memebers");
    }
}
} // namespace node
