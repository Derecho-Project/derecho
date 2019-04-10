#ifndef ROUTING_DIJKSTRA_H_
#define ROUTING_DIJKSTRA_H_

#include <limits> // for numeric_limits
#include <list>
#include <vector>


namespace sst {

/**
 * Functions, structures, and types related to path finding in abstract graphs.
 */
namespace path_finding {

typedef int vertex_t;
typedef int weight_t;

const weight_t max_weight = std::numeric_limits<int>::max();

struct neighbor {
	vertex_t target;
	weight_t weight;
	neighbor(vertex_t arg_target, weight_t arg_weight)
	: target(arg_target), weight(arg_weight) { }
};

typedef std::vector<std::vector<neighbor> > adjacency_list_t;


void DijkstraComputePaths(vertex_t source,
		const adjacency_list_t& adjacency_list,
		std::vector<weight_t>& min_distance,
		std::vector<vertex_t>& previous_vertex);


std::list<vertex_t> DijkstraGetShortestPathTo(vertex_t vertex,
		const std::vector<vertex_t> &previous_vertex);

std::list<std::pair<vertex_t, vertex_t>> PathToLinks(std::list<vertex_t> path);

} //namespace path_finding

} //namespace sst

#endif /* ROUTING_DIJKSTRA_H_ */
