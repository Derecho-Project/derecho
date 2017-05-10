#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <map>
#include <random>
#include <string>
#include <sys/time.h>
#include <tuple>
#include <vector>

#include "sst/sst.h"
#include "sst/tcp.h"
#include "sst/verbs.h"
#include "statistics.h"

static const long long int SECONDS_TO_NS = 1000000000LL;
static const double RESPONSE_TIME_THRESHOLD = 150.0;
static const int TIMING_NODE = 0;

static uint32_t num_nodes, this_node_rank;

using std::cout;
using std::endl;
using std::vector;
using std::map;
using std::string;
using std::ifstream;
using std::ofstream;
using std::tie;

struct Load_Row {
    volatile double avg_response_time;
    volatile int barrier;
};

namespace sst {
namespace tcp {
extern int port;
}
}

int main(int argc, char** argv) {
    using namespace sst;

    if(argc < 2) {
        cout << "Please provide a configuration file." << endl;
        return -1;
    }
    srand(time(NULL));

    ifstream node_config_stream;
    node_config_stream.open(argv[1]);

    // input number of nodes and the local node id
    node_config_stream >> num_nodes >> this_node_rank;

    // input the ip addresses
    map<uint32_t, string> ip_addrs;
    for(unsigned int i = 0; i < num_nodes; ++i) {
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
    for(uint32_t i = 0; i < num_nodes; ++i) {
        group_members[i] = i;
    }

    SST<Load_Row, Mode::Writes>* sst = new SST<Load_Row, Mode::Writes>(group_members, this_node_rank);
    const int local = sst->get_local_index();
    //Initialize the SST
    (*sst)[local].avg_response_time = 100.0;
    (*sst)[local].barrier = 0;
    sst->put();
    bool done = false;
    while(!done) {
        done = true;
        for(unsigned int i = 0; i < num_nodes; ++i) {
            if((*sst)[i].avg_response_time != 100.0) {
                done = false;
            }
        }
    }
    //External barrier before starting the experiment
    sst->sync_with_members();

    auto detect_load = [](const SST<Load_Row, Mode::Writes>& sst) {
        double sum = 0;
        for(unsigned int n = 0; n < num_nodes; ++n) {
            sum += sst[n].avg_response_time;
        }
        return sum / num_nodes > RESPONSE_TIME_THRESHOLD;
    };

    auto react_to_load = [](SST<Load_Row, Mode::Writes>& sst) {
        if(sst[sst.get_local_index()].barrier == sst[TIMING_NODE].barrier) {
            sst[sst.get_local_index()].barrier++;
        }
    };

    sst->predicates.insert(detect_load, react_to_load);

    if(this_node_rank == TIMING_NODE) {
        int experiment_reps = 1000;
        vector<long long int> start_times(experiment_reps), end_times(experiment_reps);

        //Predicate to detect all nodes reaching the barrier
        int current_barrier_value = 1;
        auto barrier_pred = [&current_barrier_value](const SST<Load_Row, Mode::Writes>& sst) {
            //Since node 0 is the master node, start checking at 1
            for(unsigned int n = 1; n < num_nodes; ++n) {
                if(sst[n].barrier < current_barrier_value)
                    return false;
            }
            return true;
        };

        std::uniform_int_distribution<long long int> wait_rand(2e8 + 1, 2e8 + 6e5 + 1);
        std::mt19937 engine;
        for(int rep = 0; rep < experiment_reps; ++rep) {
            cout << "Starting experiment rep " << rep << endl;
            auto barrier_action = [&current_barrier_value, &end_times, rep](SST<Load_Row, Mode::Writes>& sst) {
                struct timespec end_time;
                clock_gettime(CLOCK_REALTIME, &end_time);
                end_times[rep] = end_time.tv_sec * SECONDS_TO_NS + end_time.tv_nsec;
                cout << "All nodes have reached barrier " << current_barrier_value << endl;
                const int me = sst.get_local_index();
                //Reset load value
                sst[me].avg_response_time = 100.0;
                //Release barrier for other nodes
                sst[me].barrier = current_barrier_value;
                sst.put();
                current_barrier_value++;
            };
            sst->predicates.insert(barrier_pred, barrier_action);

            //Wait a random amount of time, then change the load reported to trigger the predicate
            long long int rand_time = wait_rand(engine);
            bool throwaway = false;
            for(long long int i = 0; i < rand_time; ++i) {
                throwaway = !throwaway;
            }

            (*sst)[local].avg_response_time = 550.0;
            sst->put();
            struct timespec start_time;
            clock_gettime(CLOCK_REALTIME, &start_time);
            start_times[rep] = start_time.tv_sec * SECONDS_TO_NS + start_time.tv_nsec;
            // wait a while for the predicate to be detected and all nodes to update the barrier
            for(long long int i = 0; i < (long long int)1e6; ++i) {
                throwaway = !throwaway;
            }
        }
        //Sync with the other nodes to ensure they're also done
        sst->sync_with_members();

        double mean, stdev;
        tie(mean, stdev) = experiments::compute_statistics(start_times, end_times);
        ofstream data_out_stream(string("router_data_" + std::to_string(num_nodes) + ".csv").c_str());
        data_out_stream << num_nodes << "," << mean << "," << stdev << endl;
        data_out_stream.close();
    } else {
        tcp::sync(TIMING_NODE);
    }

    delete(sst);
}
