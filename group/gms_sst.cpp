#include "gms_sst.hpp"

namespace group {
GMSSST::GMSSST(const node::NodeCollection& members, uint32_t max_changes)
        : sst::SST<GMSSST>(this, members),
          suspicions(members.num_nodes),
          changes(max_changes),
          joiner_ips(max_changes),
          joiner_gms_ports(max_changes),
          joiner_rpc_ports(max_changes),
          joiner_sst_ports(max_changes),
          joiner_rdmc_ports(max_changes) {
    initialize(vid, suspicions, changes,
               joiner_ips, joiner_gms_ports, joiner_rpc_ports,
               joiner_sst_ports, joiner_rpc_ports, num_changes,
               num_committed, num_acked, num_installed,
               wedged, rip);

    for(auto row = 0u; row < members.num_nodes; ++row) {
        for(auto index = 0u; index < suspicions.size(); ++index) {
            suspicions[row][index] = false;
        }
        for(auto index = 0u; index < changes.size(); ++index) {
            changes[row][index] = false;
            joiner_ips[row][index] = 0;
            joiner_gms_ports[row][index] = 0;
            joiner_rpc_ports[row][index] = 0;
            joiner_sst_ports[row][index] = 0;
            joiner_rdmc_ports[row][index] = 0;
        }
        vid[row] = 0;
        num_changes[row] = 0;
        num_committed[row] = 0;
        num_installed[row] = 0;
        num_acked[row] = 0;
        wedged[row] = false;
        rip[row] = false;
    }
    sync_with_members();
}
}  // namespace group
