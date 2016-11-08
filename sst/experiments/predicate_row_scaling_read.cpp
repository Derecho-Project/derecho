#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <map>
#include <random>
#include <string>
#include <tuple>
#include <vector>

#include "sst/sst.h"
#include "sst/tcp.h"
#include "sst/verbs.h"
#include "statistics.h"
#include "timing.h"

using std::vector;
using std::map;
using std::string;
using std::cin;
using std::cout;
using std::endl;
using std::ofstream;
using std::ifstream;
using std::tie;

struct TestRow {
  volatile int data;
};

namespace sst {
namespace tcp {
extern int port;
}
}

static uint32_t num_nodes, this_node_rank;
static const int EXPERIMENT_TRIALS = 10000;

int main (int argc, char** argv) {

	using namespace sst;

	if (argc < 2) {
		cout << "Please provide a configuration file." << endl;
		return -1;
	}
	if (argc < 3) {
		cout << "Please provide at least one value of r to test." << endl;
		return -1;
	}

	srand (time (NULL));
	cout << "Arguments were: ";
	for(int i = 0; i < argc; ++i) {
		cout << argv[i] << " ";
	}
	cout << endl;

	ifstream node_config_stream;
	node_config_stream.open (argv[1]);

	// input number of nodes and the local node id
	node_config_stream >> num_nodes >> this_node_rank;

	// input the ip addresses
	map <uint32_t, string> ip_addrs;
	for (unsigned int i = 0; i < num_nodes; ++i) {
		node_config_stream >> ip_addrs[i];
	}

	node_config_stream >> tcp::port;

	node_config_stream.close();

	// Get the values of R to test from the other arguments
	vector<int> row_counts(argc - 2);
	for(int i = 0; i < argc-2; ++i) {
		row_counts[i] = std::stoi(string(argv[i+2]));
	}

	// initialize tcp connections
	tcp::tcp_initialize(this_node_rank, ip_addrs);

	// initialize the rdma resources
	verbs_initialize();

	// make all the nodes members of a group
	vector <uint32_t> group_members (num_nodes);
	for (uint32_t i = 0; i < num_nodes; ++i) {
		group_members[i] = i;
	}
	// create a new shared state table with all the members
	SST<TestRow, Mode::Reads> sst(group_members, this_node_rank);
	const int local = sst.get_local_index();
	sst[local].data = 0;

	//Run the experiment for each value of R
	for(unsigned int r : row_counts) {

		if(this_node_rank == 0) {
			cout << "Starting experiment for r=" << r << endl;
			vector<long long int> start_times(EXPERIMENT_TRIALS);
			vector<long long int> end_times(EXPERIMENT_TRIALS);
			auto experiment_pred = [r](const SST<TestRow, Mode::Reads>& sst) {
				for(unsigned int n = 0; n <= r; ++n) {
					if(sst[n].data == 0) {
						return false;
					}
				}
				return true;
			};
			std::uniform_int_distribution<long long int> wait_rand(10 * MILLIS_TO_NS, 20 * MILLIS_TO_NS);
			std::mt19937 engine;
			for(int trial = 0; trial < EXPERIMENT_TRIALS; ++trial) {

				auto done_action = [&end_times, trial](SST<TestRow, Mode::Reads>& sst) {
					end_times[trial] = experiments::get_realtime_clock();
					sst[sst.get_local_index()].data = 0;
				};
				
				sst.predicates.insert(experiment_pred, done_action, PredicateType::ONE_TIME);
				
				//Wait for everyone to be ready before starting
				sst.sync_with_members();
				long long int rand_time = wait_rand(engine);
				experiments::busy_wait_for(rand_time);

				start_times[trial] = experiments::get_realtime_clock();
				sst[local].data = 1;

				//Wait for the predicate to be detected (i.e. the experiment to end)
				experiments::busy_wait_for(1 * MILLIS_TO_NS);

				//Make sure experiment is done
				while(sst[local].data != 0) {
				}

				//Let the remote nodes know they can proceed
				sst.sync_with_members();
			}

			cout << "Experiment finished for r=" << r << endl;
			double mean, stdev;
			tie(mean, stdev) = experiments::compute_statistics(start_times, end_times);
			ofstream data_out_stream(string("predicate_complexity_read_" + std::to_string(num_nodes) + ".csv").c_str(), ofstream::app);
			data_out_stream << r << "," << mean << "," << stdev << endl;
			data_out_stream.close();

		} else {

			for(int trial = 0; trial < EXPERIMENT_TRIALS; ++trial) {

				if(this_node_rank < r) {
					sst[local].data = 1;
				} else {
					sst[local].data = 0;
				}

				if(this_node_rank == r) {
					//Predicate to detect that node 0 is ready to start the experiment
					auto start_pred = [](const SST<TestRow, Mode::Reads>& sst) {
						return sst[0].data == 1;
					};

					//Change this node's value to 1 in response
					auto start_react = [](SST<TestRow, Mode::Reads>& sst) {
						sst[sst.get_local_index()].data = 1;
					};
					sst.predicates.insert(start_pred, start_react, PredicateType::ONE_TIME);
				}

				//Wait for everyone to be ready before starting
				sst.sync_with_members();


				//Wait for node 0 to finish the trial
				tcp::sync(0);
			}

		}
	}

	return 0;
}
