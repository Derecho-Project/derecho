#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <map>
#include <random>
#include <string>
#include <tuple>
#include <vector>

#include "sst/predicates.h"
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

static uint32_t num_nodes, this_node_rank;
static const int EXPERIMENT_TRIALS = 10000;

int main(int argc, char** argv) {
    using namespace sst;

    if(argc < 2) {
        cout << "Please provide a configuration file." << endl;
        return -1;
    }
    if(argc < 3) {
        cout << "Please provide at least one value of r to test." << endl;
        return -1;
    }

    srand(time(NULL));
    cout << "Arguments were: ";
    for(int i = 0; i < argc; ++i) {
        cout << argv[i] << " ";
    }
    cout << endl;

    ifstream node_config_stream;
    node_config_stream.open(argv[1]);

    cout << "BEWARE!!! node rank must be given before num nodes as input" << endl;
    // input number of nodes and the local node id
    node_config_stream >> this_node_rank >> num_nodes;

    // input the ip addresses
    map<uint32_t, string> ip_addrs;
    for(unsigned int i = 0; i < num_nodes; ++i) {
        node_config_stream >> ip_addrs[i];
    }

    node_config_stream.close();

    // Get the values of R to test from the other arguments
    vector<int> row_counts(argc - 2);
    for(int i = 0; i < argc - 2; ++i) {
        row_counts[i] = std::stoi(string(argv[i + 2]));
    }

    // initialize tcp connections
    tcp::tcp_initialize(this_node_rank, ip_addrs);

    // initialize the rdma resources
    verbs_initialize();

    // make all the nodes members of a group
    vector<uint32_t> group_members(num_nodes);
    for(uint32_t i = 0; i < num_nodes; ++i) {
        group_members[i] = i;
    }
    // create a new shared state table with all the members
    SST<TestRow> sst(group_members, this_node_rank);
    const int local = sst.get_local_index();
    sst[local].data = 0;
    sst.put();

    //Run the experiment for each value of R
    for(unsigned int r : row_counts) {
        if(this_node_rank == 0) {
            cout << "Starting experiment for r=" << r << endl;
            vector<long long int> start_times(EXPERIMENT_TRIALS);
            vector<long long int> end_times(EXPERIMENT_TRIALS);
            auto experiment_pred = [r](const SST<TestRow>& sst) {
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
                // cout << "Trial number " << trial << endl;

                auto done_action = [&end_times, trial](SST<TestRow>& sst) {
                    end_times[trial] = experiments::get_realtime_clock();
                    sst[sst.get_local_index()].data = 0;
                    sst.put();
                };

                sst.predicates.insert(experiment_pred, done_action, PredicateType::ONE_TIME);

                //Wait for everyone to be ready before starting
                sst.sync_with_members();
                long long int rand_time = wait_rand(engine);
                experiments::busy_wait_for(rand_time);

                start_times[trial] = experiments::get_realtime_clock();
                sst[local].data = 1;
                sst.put();

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
            ofstream data_out_stream(string("predicate_complexity_" + std::to_string(num_nodes) + ".csv").c_str(), ofstream::app);
            data_out_stream << r << "," << mean << "," << stdev << endl;
            data_out_stream.close();

        } else {
            for(int trial = 0; trial < EXPERIMENT_TRIALS; ++trial) {
                if(this_node_rank < r) {
                    sst[local].data = 1;
                    sst.put();
                } else {
                    sst[local].data = 0;
                    sst.put();
                }

                if(this_node_rank == r) {
                    //Predicate to detect that node 0 is ready to start the experiment
                    auto start_pred = [](const SST<TestRow>& sst) {
                        return sst[0].data == 1;
                    };

                    //Change this node's value to 1 in response
                    auto start_react = [](SST<TestRow>& sst) {
                        sst[sst.get_local_index()].data = 1;
                        sst.put();
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
