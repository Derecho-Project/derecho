#include "dijkstra.hpp"

#include <iterator>
#include <set>
#include <utility>
#include <iostream>

namespace sst {

namespace path_finding {

using std::list;
using std::vector;
using std::pair;
using std::make_pair;

void DijkstraComputePaths(vertex_t source,
		const adjacency_list_t& adjacency_list,
		vector<weight_t>& min_distance,
		vector<vertex_t>& previous_vertex)
{
	int n = adjacency_list.size();
	min_distance.clear();
	min_distance.resize(n, max_weight);
	min_distance[source] = 0;
	previous_vertex.clear();
	previous_vertex.resize(n, -1);
	std::set<std::pair<weight_t, vertex_t>> vertex_queue;
	vertex_queue.insert(make_pair(min_distance[source], source));

	while (!vertex_queue.empty())
	{
		weight_t dist = vertex_queue.begin()->first;
		vertex_t u = vertex_queue.begin()->second;
		vertex_queue.erase(vertex_queue.begin());

		// Visit each edge exiting u
		const vector<neighbor>& neighbors = adjacency_list[u];
		for (vector<neighbor>::const_iterator neighbor_iter = neighbors.begin();
				neighbor_iter != neighbors.end();
				neighbor_iter++)
		{
			vertex_t v = neighbor_iter->target;
			weight_t weight = neighbor_iter->weight;
			weight_t distance_through_u = dist + weight;
			if (distance_through_u < min_distance[v]) {
				vertex_queue.erase(make_pair(min_distance[v], v));

				min_distance[v] = distance_through_u;
				previous_vertex[v] = u;
				vertex_queue.insert(make_pair(min_distance[v], v));

			}

		}
	}
}


list<vertex_t> DijkstraGetShortestPathTo(vertex_t vertex,
		const vector<vertex_t> &previous_vertex) {
	list<vertex_t> path;
	for ( ; vertex != -1; vertex = previous_vertex[vertex])
		path.push_front(vertex);
	return path;
}

list<pair<vertex_t, vertex_t>> PathToLinks(list<vertex_t> path) {
	list<pair<vertex_t, vertex_t>> links;
	for(list<vertex_t>::const_iterator iter = path.begin(); iter != std::prev(path.end()); ++iter) {
		links.push_back(make_pair(*iter, *(std::next(iter))));
	}
	return links;
}

} //namespace path_finding

} //namespace sst

/*
//TESTING

int main()
{
	using namespace path_finding;
	using namespace std;

    adjacency_list_t adjacency_list(8);
    adjacency_list[0].push_back(neighbor(1, 1));
    adjacency_list[0].push_back(neighbor(2, 1));
    adjacency_list[0].push_back(neighbor(3, 1));
    adjacency_list[0].push_back(neighbor(4, 1));
    adjacency_list[0].push_back(neighbor(5, 1));
    adjacency_list[0].push_back(neighbor(6, 1));
    adjacency_list[0].push_back(neighbor(7, 1));

    adjacency_list[1].push_back(neighbor(0, 1));
    adjacency_list[1].push_back(neighbor(2, 3));
    adjacency_list[1].push_back(neighbor(7, 3));

    adjacency_list[2].push_back(neighbor(0, 1));
    adjacency_list[2].push_back(neighbor(1, 3));
    adjacency_list[2].push_back(neighbor(3, 3));


    adjacency_list[3].push_back(neighbor(0, 1));
    adjacency_list[3].push_back(neighbor(2, 3));
    adjacency_list[3].push_back(neighbor(4, 3));

    adjacency_list[4].push_back(neighbor(0, 1));
    adjacency_list[4].push_back(neighbor(3, 3));
    adjacency_list[4].push_back(neighbor(5, 3));

    adjacency_list[5].push_back(neighbor(0, 1));
    adjacency_list[5].push_back(neighbor(4, 3));
    adjacency_list[5].push_back(neighbor(6, 3));

    adjacency_list[6].push_back(neighbor(0, 1));
    adjacency_list[6].push_back(neighbor(5, 3));
    adjacency_list[6].push_back(neighbor(7, 3));

    adjacency_list[7].push_back(neighbor(0, 1));
    adjacency_list[7].push_back(neighbor(6, 3));
    adjacency_list[7].push_back(neighbor(1, 3));

    vector<weight_t> min_distance;
    vector<vertex_t> previous;
    DijkstraComputePaths(2, adjacency_list, min_distance, previous);
    cout << "Distance from 2 to 1: " << min_distance[1] << endl;
    list<vertex_t> path = DijkstraGetShortestPathTo(1, previous);
    cout << "Path : ";
    copy(path.begin(), path.end(), ostream_iterator<vertex_t>(cout, " "));
    cout << endl;
    cout << "Distance from 2 to 3: " << min_distance[3] << std::endl;
    path = DijkstraGetShortestPathTo(3, previous);
    cout << "Path : ";
    copy(path.begin(), path.end(), ostream_iterator<vertex_t>(cout, " "));
    cout << endl;

    return 0;
}

*/
