#pragma once

#include <map>

#include "node.hpp"
#include "node_collection.hpp"

namespace node {
class NodeCollectionWithSenders : public NodeCollection {
    std::map<node_id_t, uint32_t> node_id_to_sender_rank;
public:
    NodeCollectionWithSenders(const std::vector<node_id_t>& nodes, const node_id_t my_id, const std::vector<bool> senders_info);

    const uint32_t num_senders;
    // -1 if this node is not a sender
    const int32_t my_sender_rank;
    const std::vector<uint32_t> ranks_of_senders;

    // -1 if node is not a sender
    int32_t get_sender_rank_of(node_id_t node_id) const;
};
}
