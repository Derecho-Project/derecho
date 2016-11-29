#include <iostream>
#include <map>
#include <vector>
#include <fstream>
#include <ctime>
#include <string>

#include "sst/sst.h"
#include "sst/tcp.h"
#include "derecho/experiments//timing.h"
#include "derecho/experiments/statistics.h"

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
  volatile int flag;
};

namespace sst {
namespace tcp {
extern int port;
}
}

static int num_nodes, this_node_rank;

static const int TIMING_NODE = 0;

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
	cout << "Arguments were: ";
	for(int i = 0; i < argc; ++i) {
		cout << argv[i] << " ";
	}
	cout << endl;

	srand (time (NULL));

	ifstream node_config_stream;
	node_config_stream.open (argv[1]);

	// input number of nodes and the local node id
	node_config_stream >> num_nodes >> this_node_rank;

	// input the ip addresses
	map <uint32_t, string> ip_addrs;
	for (int i = 0; i < num_nodes; ++i) {
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
	for (int i = 0; i < num_nodes; ++i) {
		group_members[i] = i;
	}

	// create a new shared state table with all the members
	SST<TestRow>* sst = new SST<TestRow> (group_members, this_node_rank);
	const int local = sst->get_local_index();
	if(this_node_rank == TIMING_NODE) {
		(*sst)[local].flag = 1;
	} else {
		(*sst)[local].flag = 0;
	}
	sst->put();

	//Make sure initial writes are finished
	sst->sync_with_members();
	//Warm up the processor
	experiments::busy_wait_for(3 * SECONDS_TO_NS);

	int r = 0;
	long long int count = 0;


	if(this_node_rank == TIMING_NODE) {
		auto test_pred = [&r] (const SST<TestRow>& sst) {
			for(int n = 0; n <= r; ++n) {
				if(sst[n].flag != 0) {
					return false;
				}
			}
			return true;
		};
		auto count_action = [&count] (SST<TestRow>& sst) {
			++count;
		};

		sst->predicates.insert(test_pred, count_action, PredicateType::RECURRENT);
	}

	//Run the experiment for each value of R
	for(int rowcount : row_counts) {
		r = rowcount;

		if(this_node_rank == TIMING_NODE) {
			count = 0;
			
			//Trigger the predicate to start being true by setting my own value to 0
			(*sst)[local].flag = 0;
			long long int start_time = experiments::get_realtime_clock();
			experiments::busy_wait_for(100000000);
			//Stop the predicate by setting my value to 1
			(*sst)[local].flag = 1;
			long long int end_time = experiments::get_realtime_clock();

			long long int actual_run_time = end_time-start_time;
			ofstream data_out_stream(string("predicates_per_sec_" + std::to_string(num_nodes) + ".csv").c_str(), ofstream::app);
			data_out_stream << r << "," << count << "," << actual_run_time << endl;
			data_out_stream.close();
		
		}
	}

	if(this_node_rank == TIMING_NODE) {
		sst->sync_with_members();
	} else {
		//Other nodes will just block here until the end, they don't have anything to do
		tcp::sync(TIMING_NODE);
	}

	return 0;
}

