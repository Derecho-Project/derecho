#include "node_collection_with_senders.hpp"
#include <numeric>

namespace node {
uint32_t get_num_senders(const std::vector<bool> senders_info) {
    return std::accumulate(senders_info.begin(), senders_info.end(), 0,
                           [](int sum, bool is_sender) {
                               return sum + (is_sender ? 1 : 0);
                           });
}

int32_t get_my_sender_rank(const std::vector<bool> senders_info, const uint32_t my_rank) {
    if(!senders_info[my_rank]) {
        return -1;
    }
    return std::accumulate(senders_info.begin(), senders_info.begin() + my_rank, 0,
                           [](int sum, bool is_sender) {
                               return sum + (is_sender ? 1 : 0);
                           });
}

std::vector<uint32_t> get_ranks_of_senders(const std::vector<bool> senders_info) {
    std::vector<uint32_t> ranks_of_senders;
    for(uint32_t rank = 0; rank < senders_info.size(); ++rank) {
        if(senders_info[rank]) {
            ranks_of_senders.push_back(rank);
        }
    }
    return ranks_of_senders;
}

NodeCollectionWithSenders::NodeCollectionWithSenders(const std::vector<node_id_t>& nodes, const node_id_t my_id, const std::vector<bool> senders_info)
        : NodeCollection(nodes, my_id),
          num_senders(get_num_senders(senders_info)),
          my_sender_rank(get_my_sender_rank(senders_info, my_rank)),
          ranks_of_senders(get_ranks_of_senders(senders_info)) {
    for(uint32_t sender_rank = 0; sender_rank < num_senders; ++sender_rank) {
        node_id_to_sender_rank[nodes[ranks_of_senders[sender_rank]]] = sender_rank;
    }
}

int32_t NodeCollectionWithSenders::get_sender_rank_of(node_id_t node_id) const {
    if(node_id_to_sender_rank.find(node_id) != node_id_to_sender_rank.end()) {
      return node_id_to_sender_rank.at(node_id);
    }
    return -1;
}
} // namespace node
