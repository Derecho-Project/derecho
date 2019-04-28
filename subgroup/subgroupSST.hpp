#pragma once

#include "msg/msg.hpp"
#include "node/nodeCollection.hpp"
#include "sst/sst.hpp"

namespace subgroup {
using sst::SSTField;
using sst::SSTFieldVector;
using msg::msg_id_t;

class SubgroupSST : public sst::SST<SubgroupSST> {
public:
    /**
     * Local count of number of received messages by sender.  For each
     * sender k, received_num[k] is the number received (a.k.a. "locally stable").
     */
    SSTFieldVector<int32_t> received_num;
    // MulticastGroup members, related only to tracking message delivery
    /**
     * Sequence numbers are interpreted like a row-major pair:
     * (sender, index) becomes sender + num_members * index.
     * Since the global order is round-robin, the correct global order of
     * messages becomes a consecutive sequence of these numbers: with 4
     * senders, we expect to receive (0,0), (1,0), (2,0), (3,0), (0,1),
     * (1,1), ... which is 0, 1, 2, 3, 4, 5, ....
     *
     * This variable is the highest sequence number that has been received
     * in-order by this node; if a node updates seq_num, it has received all
     * messages up to seq_num in the global round-robin order.
     */
    SSTField<msg_id_t> seq_num;
    /**
     * This represents the highest sequence number that has been delivered
     * at this node. Messages are only delivered once stable (received by all),
     * so it must be at least stable_num.
     */
    SSTField<msg_id_t> delivered_num;
    /**
     * This represents the highest persistent version number that has been
     * persisted to disk at this node, if persistence is enabled. This is
     * updated by the PersistenceManager.
     */
    SSTField<msg_id_t> persisted_num;
    /**
     * reports that this member in this subgroup is wedged.
     * Must be after num_received!
     */
    SSTField<bool> wedged;
    /**
     * Array of how many messages to accept from each sender in the current
     * view change.
     */
    SSTFieldVector<msg_id_t> min_accepted_num;
    /**
     * Boolean indicating whether the shard leader
     * has published a ragged edge for the current view change
     */
    SSTField<bool> ragged_edge_computed;

    SubgroupSST(node::NodeCollection subgroup_members);
};
}  // namespace subgroup
