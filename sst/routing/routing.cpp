#include "routing.h"

#include <stddef.h>
#include <iostream>
#include <list>
#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>
#include <cassert>

#include "dijkstra.h"
#include "lsdb_row.h"
#include "std_hashes.h"

using std::cout;
using std::endl;
using std::vector;
using std::string;
using std::unordered_set;
using std::pair;
using std::make_pair;
using std::tie;
using std::string;

namespace sst {

namespace experiments {

/**
 * @details
 * Constructs a graph of network connectivity from the given link-state
 * database, then runs Dijkstra's algorithm on it to find the shortest path to
 * each node. The outgoing first hop for each node is set to the first node on
 * the shortest path to that destination.
 *
 * @param[in] this_node_num The node rank of the local node, which is the
 * origin of all paths for routing.
 * @param[in] num_nodes The number of nodes in the system; equal to the size of
 * the routing table
 * @param[out] forwarding_table The routing table, which maps destination node
 * ranks (the index) to the first hop on the path to that destination (the
 * value).
 * @param[out] links_used The set of links used in all the routing paths used
 * to compute this table; a pair (x,y) represents the link from node x to node
 * y.
 * @param[in] linkstate_rows The link state table, which is a snapshot of the
 * link state SST.
 */
void compute_routing_table(int this_node_num, int num_nodes, vector<int>& forwarding_table, unordered_set<pair<int, int>>& links_used,
        RoutingSST::SST_Snapshot& linkstate_rows) {
    assert(forwarding_table.size() == static_cast<vector<int>::size_type>(num_nodes));
	//Build a graph of the network from the link state table, then pass it to Dijkstra
	path_finding::adjacency_list_t network_adjacency_list(num_nodes);
	for (int source = 0; source < num_nodes; ++source) {
		for (int target = 0; target < num_nodes; ++target) {
			if (source != target && linkstate_rows[source].link_cost[target] > 0) {
				network_adjacency_list[source].push_back(
						path_finding::neighbor(target, linkstate_rows[source].link_cost[target]));
			}
		}
	}
	vector<path_finding::weight_t> min_distance; //Unused output parameter
	vector<path_finding::vertex_t> previous_vertexes;
	path_finding::DijkstraComputePaths(this_node_num, network_adjacency_list,
			min_distance, previous_vertexes);

	//Clear and re-populate the routing table and list of links used
	links_used.clear();
	for (int dest_node = 0; dest_node < num_nodes; ++dest_node) {
		if (dest_node == this_node_num) {
			forwarding_table[dest_node] = dest_node;
			continue;
		}
		auto path = path_finding::DijkstraGetShortestPathTo(dest_node, previous_vertexes);
		auto links = path_finding::PathToLinks(path);
		links_used.insert(links.begin(), links.end());
		path.pop_front(); //First element of path is always the origin
		forwarding_table[dest_node] = path.front();
	}
}

/**
 * @param forwarding_table The routing table to print, as a vector mapping
 * destination node ranks to first hops on the path.
 */
void print_routing_table(vector<int>& forwarding_table) {
	cout << "Destination  Route Via" << endl;
	for(size_t i = 0; i < forwarding_table.size(); ++i) {
		cout << i << "\t\t" << forwarding_table[i] << endl;
	}
}

} //namespace experiments

} //namespace sst

