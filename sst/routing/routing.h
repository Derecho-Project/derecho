#ifndef ROUTING_ROUTING_H_
#define ROUTING_ROUTING_H_

#include <unordered_set>
#include <utility>
#include <vector>

#include "sst/sst.h"
#include "lsdb_row.h"
#include "std_hashes.h"

static constexpr int RACK_SIZE = 30;

namespace sst {

namespace experiments {

/** Type definition for the SST used in the router implementation. */
using RoutingSST = SST<LSDB_Row<RACK_SIZE>, Mode::Writes>;

/** Creates or updates a routing table, stored in `forwarding_table`, based on
 * the link state information provided in `linkstate_rows`.*/
void compute_routing_table(int this_node_num, int num_nodes, std::vector<int>& forwarding_table, std::unordered_set<std::pair<int, int>>& links_used,
		RoutingSST::SST_Snapshot& linkstate_rows);

/** Prints a routing table to stdout. */
void print_routing_table(std::vector<int>& forwarding_table);

}
}



#endif /* ROUTING_ROUTING_H_ */
