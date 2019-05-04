#include "node/node_collection_with_senders.hpp"
#include "exception/node_exceptions.hpp"
#include "p2p_connections.hpp"

namespace p2p {
P2PConnections::P2PConnections(const node::NodeCollection& members, size_t max_payload_size, size_t window_size, const msg::recv_upcall_t& receive_upcall)
        : members(members),
          connections(members.num_nodes),
          max_payload_size(max_payload_size),
          window_size(window_size),
          receive_upcall(receive_upcall) {
    for(auto other_rank : members.other_ranks) {
        node::NodeCollectionWithSenders pair_of_nodes(other_rank < members.my_rank ? std::vector<node::node_id_t>{members[other_rank], members.my_id}
                                                                                   : std::vector<node::node_id_t>{members.my_id, members[other_rank]},
                                                      members.my_id, {true, true});
        connections[other_rank] = std::make_unique<smc::SMCGroup>(pair_of_nodes, max_payload_size,
                                                                  window_size, receive_upcall);
    }
}

P2PConnections::P2PConnections(P2PConnections&& oldP2PConnections, const node::NodeCollection& members)
        : members(members),
          connections(members.num_nodes),
          max_payload_size(oldP2PConnections.max_payload_size),
          window_size(oldP2PConnections.window_size),
          receive_upcall(oldP2PConnections.receive_upcall) {
    for(auto other_rank : members.other_ranks) {
        try {
            const uint32_t old_rank = oldP2PConnections.members.get_rank_of(members[other_rank]);
            connections[other_rank] = std::move(oldP2PConnections.connections[old_rank]);
        } catch(node::IDNotInMembers) {
            node::NodeCollectionWithSenders pair_of_nodes(other_rank < members.my_rank ? std::vector<node::node_id_t>{members[other_rank], members.my_id}
                                                                                       : std::vector<node::node_id_t>{members.my_id, members[other_rank]},
                                                          members.my_id, {true, true});
            connections[other_rank] = std::make_unique<smc::SMCGroup>(pair_of_nodes, max_payload_size,
                                                                      window_size, receive_upcall);
        }
    }
}
}  // namespace p2p
