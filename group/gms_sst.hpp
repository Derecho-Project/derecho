#pragma once

#include "sst/sst.hpp"
#include "node/nodeCollection.hpp"

namespace group {
using sst::SSTField;
using sst::SSTFieldVector;

class GMSSST : public sst::SST<GMSSST> {
public:
    // Group management service members, related only to handling view changes
    /** View ID associated with this SST. VIDs monotonically increase as views change. */
    SSTField<int32_t> vid;
    /**
     * Array of same length as View::members, where each bool represents
     * whether the corresponding member is suspected to have failed
     */
    SSTFieldVector<bool> suspicions;
    /**
     * An array of the same length as View::members, containing a list of
     * proposed changes to the view that have not yet been installed. The number
     * of valid elements is num_changes - num_installed, which should never exceed
     * View::num_members/2.
     * If request i is a Join, changes[i] is not in current View's members.
     * If request i is a Departure, changes[i] is in current View's members.
     */
    SSTFieldVector<node::node_id_t> changes;
    /**
     * If changes[i] is a Join, joiner_ips[i] is the IP address of the joining
     * node, packed into an unsigned int in network byte order. This
     * representation is necessary because SST doesn't support variable-length
     * strings.
     */
    SSTFieldVector<uint32_t> joiner_ips;
    /** joiner_xxx_ports are the port numbers for the joining nodes. */
    SSTFieldVector<uint16_t> joiner_gms_ports;
    SSTFieldVector<uint16_t> joiner_rpc_ports;
    SSTFieldVector<uint16_t> joiner_sst_ports;
    SSTFieldVector<uint16_t> joiner_rdmc_ports;
    /**
     * How many changes to the view have been proposed. Monotonically increases.
     * num_changes - num_committed is the number of pending changes, which should never
     * exceed the number of members in the current view. If num_changes == num_committed
     * == num_installed, no changes are pending.
     */
    SSTField<int> num_changes;
    /** How many proposed view changes have reached the commit point. */
    SSTField<int> num_committed;
    /**
     * How many proposed changes have been seen. Incremented by a member
     * to acknowledge that it has seen a proposed change.
     */
    SSTField<int> num_acked;
    /**
     * How many previously proposed view changes have been installed in the
     * current view. Monotonically increases, lower bound on num_committed.
     */
    SSTField<int> num_installed;
    /** Set after wedging every subgroup */
    SSTField<bool> wedged;
    /** to signal a graceful exit */
    SSTField<bool> rip;

    GMSSST(const node::NodeCollection& members, uint32_t max_changes);
};
}  // namespace group
