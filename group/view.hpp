#pragma once

#include <vector>

#include "node/node_collection.hpp"
#include "tcp/tcp.hpp"

namespace group {

class View {
    using tcp::ip_addr_t;
    using tcp::port_t;
    friend class ViewManager;
    /** Sequential view ID: 0, 1, ... */
    const uint32_t vid;
    /** Members in the current view. */
    const node::NodeCollection members;
    /** IP addresses and ports (gms, rpc, sst, rdmc in order) of members in the current view, indexed by their rank. */
    const std::vector<std::tuple<ip_addr_t, port_t, port_t, port_t, port_t>> member_ips_and_ports;
    /** failed[i] is true if members[i] is considered to have failed.
     * Once a member is failed, it will be removed from the members list in a future view. */
    std::vector<bool> failures_info;
    /** Number of current outstanding failures in this view. After
     * transitioning to a new view that excludes some failed members, this count
     * will decrease appropriately. */
    int32_t num_failures;
    /* what I believe to be the current leader */
    uint32_t leader_rank;

    View(uint32_t vid, const node::NodeCollection& members,
         const std::vector<std::tuple<ip_addr_t, port_t, port_t, port_t, port_t>>& member_ips_and_ports,
         const std::vector<bool>& failures_info);
}
}  // namespace group
