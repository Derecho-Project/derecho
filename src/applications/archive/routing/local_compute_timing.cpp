#include <libio.h>
#include <fstream>
#include <string>
#include <tuple>
#include <vector>

#include "sst/experiments/statistics.h"
#include "routing.hpp"
#include "lsdb_row.hpp"


using std::tie;
using std::string;
using std::vector;
using std::pair;
using std::unordered_set;

static const long long int SECONDS_TO_NS = 1000000000LL;
static const int EXPERIMENT_REPS = 1000;

void time_recompute_table(int num_nodes, vector<long long int>& start_times, std::vector<long long int>& end_times) {
	using namespace sst;
	//Build a mock SST using the initial configuration files that all the nodes would have
	vector<experiments::LSDB_Row<RACK_SIZE>> linkstate_rows(num_nodes);
	for(int node = 0; node < num_nodes; ++node) {
		std::ifstream router_config_stream(string("configs/" +
				std::to_string(num_nodes) + "_connections_" + std::to_string(node)).c_str());

		int dest, cost;
		while(router_config_stream.peek() != EOF) {
			router_config_stream >> dest >> cost;
			linkstate_rows[node].link_cost[dest] = cost;

		}
	}
	vector<int> forwarding_table(num_nodes);
	unordered_set<pair<int, int>> links_used;
	//Pre-fill links_used so that clearing it takes time
	experiments::compute_routing_table(1, forwarding_table, links_used, linkstate_rows);
	//Make the changes that would be made in the experiment
	linkstate_rows[0].link_cost[1] = 10;
	linkstate_rows[0].link_cost[2] = 5;

	for(int rep = 0; rep < EXPERIMENT_REPS; ++rep) {
		struct timespec start_time;
		clock_gettime(CLOCK_REALTIME, &start_time);
		start_times[rep] = start_time.tv_sec * SECONDS_TO_NS + start_time.tv_nsec;
		experiments::compute_routing_table(1, forwarding_table, links_used, linkstate_rows);
		struct timespec end_time;
		clock_gettime(CLOCK_REALTIME, &end_time);
		end_times[rep] = end_time.tv_sec * SECONDS_TO_NS + end_time.tv_nsec;
	}
}

int main (int argc, char** argv) {

	std::ofstream data_out_stream(string("dijkstra_timing.csv").c_str());

	for(int num_nodes = 3; num_nodes <= 15; ++num_nodes) {
		std::vector<long long int> start_times(EXPERIMENT_REPS), end_times(EXPERIMENT_REPS);
		time_recompute_table(num_nodes, start_times, end_times);
		double mean, stdev;
		tie(mean, stdev) = sst::experiments::compute_statistics(start_times, end_times);
		data_out_stream << num_nodes << "," << mean << "," << stdev << std::endl;
	}

	data_out_stream.close();
}

