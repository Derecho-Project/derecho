#include <cstddef>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <map>
#include <random>
#include <string>
#include <tuple>
#include <vector>

#include "row_size_scaling.h"
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

struct BigRow {
    volatile int data[ROWSIZE];
};

namespace sst {
namespace tcp {
extern int port;
}
}

static int num_nodes, this_node_rank;
static const int EXPERIMENT_TRIALS = 10000;
static const int TIMING_NODE = 0;

int main(int argc, char** argv) {
    using namespace sst;

    if(argc < 2) {
        cout << "Please provide a configuration file." << endl;
        return -1;
    }

    ifstream node_config_stream;
    node_config_stream.open(argv[1]);

    // input number of nodes and the local node id
    node_config_stream >> num_nodes >> this_node_rank;

    // input the ip addresses
    map<uint32_t, string> ip_addrs;
    for(int i = 0; i < num_nodes; ++i) {
        node_config_stream >> ip_addrs[i];
    }

    node_config_stream >> tcp::port;

    node_config_stream.close();

    // initialize tcp connections
    tcp::tcp_initialize(this_node_rank, ip_addrs);

    // initialize the rdma resources
    verbs_initialize();

    // make all the nodes members of a group
    vector<uint32_t> group_members(num_nodes);
    for(int i = 0; i < num_nodes; ++i) {
        group_members[i] = i;
    }
    // create a new shared state table with all the members
    SST<BigRow>* sst = new SST<BigRow>(group_members, this_node_rank);
    const int local = sst->get_local_index();

    //initialize
    for(int i = 0; i < ROWSIZE; ++i) {
        (*sst)[local].data[i] = 1;
    }
    if(this_node_rank == TIMING_NODE) {
        (*sst)[local].data[0] = 0;
    }
    sst->put();

    sst->sync_with_members();

    if(this_node_rank == TIMING_NODE) {
        vector<long long int> start_times(EXPERIMENT_TRIALS);
        vector<long long int> end_times(EXPERIMENT_TRIALS);
        auto experiment_pred = [](const SST<BigRow>& sst) {
            for(int n = 0; n < num_nodes; ++n) {
                for(int i = 0; i < ROWSIZE; ++i) {
                    if(sst[n].data[i] == 0) {
                        return false;
                    }
                }
            }
            return true;
        };
        std::uniform_int_distribution<long long int> wait_rand(10 * MILLIS_TO_NS, 20 * MILLIS_TO_NS);
        std::mt19937 engine;
        for(int trial = 0; trial < EXPERIMENT_TRIALS; ++trial) {
            auto done_action = [&end_times, trial](SST<BigRow>& sst) {
                end_times[trial] = experiments::get_realtime_clock();
                sst[sst.get_local_index()].data[0] = 0;
                sst.put(offsetof(BigRow, data[0]), sizeof(int));
            };

            sst->predicates.insert(experiment_pred, done_action, PredicateType::ONE_TIME);
            //Wait for everyone to be ready before starting
            sst->sync_with_members();
            long long int rand_time = wait_rand(engine);
            experiments::busy_wait_for(rand_time);

            start_times[trial] = experiments::get_realtime_clock();
            (*sst)[local].data[0] = 1;
            sst->put(offsetof(BigRow, data[0]), sizeof(int));

            //Wait for the predicate to be detected (i.e. the experiment to end)
            experiments::busy_wait_for(1 * MILLIS_TO_NS);

            //Make sure experiment is done
            while((*sst)[local].data[0] != 0) {
            }

            //Let the remote nodes know they can proceed
            sst->sync_with_members();
        }

        double mean, stdev;
        tie(mean, stdev) = experiments::compute_statistics(start_times, end_times);
        ofstream data_out_stream(string("row_scaling_writes_" + std::to_string(num_nodes) + ".csv").c_str(), ofstream::app);
        data_out_stream << ROWSIZE << "," << mean << "," << stdev << endl;
        data_out_stream.close();

    } else {
        for(int trial = 0; trial < EXPERIMENT_TRIALS; ++trial) {
            //Initialize all columns to 1, except the last column of the last row
            for(int i = 0; i < ROWSIZE; ++i) {
                (*sst)[local].data[i] = 1;
            }
            if(this_node_rank == num_nodes - 1) {
                (*sst)[local].data[ROWSIZE - 1] = 0;
            }
            sst->put();

            if(this_node_rank == num_nodes - 1) {
                //Predicate to detect that node 0 is ready to start the experiment
                auto start_pred = [](const SST<BigRow>& sst) {
                    return sst[TIMING_NODE].data[0] == 1;
                };

                //Change this node's last value to 1 in response
                auto start_react = [](SST<BigRow>& sst) {
                    sst[sst.get_local_index()].data[ROWSIZE - 1] = 1;
                    sst.put(offsetof(BigRow, data[ROWSIZE - 1]), sizeof(int));
                };
                sst->predicates.insert(start_pred, start_react, PredicateType::ONE_TIME);
            }

            //Wait for everyone to be ready before starting
            sst->sync_with_members();

            //Wait for node 0 to finish the trial
            tcp::sync(0);
        }
    }
}
