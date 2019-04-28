#include <chrono>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <map>

#include "node/node.hpp"
#include "node/nodeCollection.hpp"
#include "sst/sst.hpp"

using std::cin;
using std::cout;
using std::endl;
using std::map;
using std::ofstream;
using std::string;
using std::vector;

using namespace sst;

class mySST : public SST<mySST> {
public:
    mySST(node::NodeCollection nodes) : SST<mySST>(this, nodes) {
        initialize(count, time);
    }
    SSTField<uint32_t> count;
    SSTField<double> time;
};

int main() {
    // input number of nodes and the local node rank
    std::cout << "Enter my_id and num_nodes" << std::endl;
    node::node_id_t my_id;
    uint32_t num_nodes;
    cin >> my_id >> num_nodes;

    std::cout << "Input the IP addresses" << std::endl;
    uint16_t port = 32567;
    // input the ip addresses
    map<uint32_t, std::pair<std::string, uint16_t>> ip_addrs_and_ports;
    for(auto i = 0u; i < num_nodes; ++i) {
      std::string ip;
      cin >> ip;
      ip_addrs_and_ports[i] = {ip, port};
    }
    std::cout << "Using the default port value of " << port << std::endl;

    rdma::initialize(my_id, ip_addrs_and_ports);

    // form a group with a subset of all the nodes
    vector<uint32_t> members(num_nodes);
    for(uint32_t i = 0; i < num_nodes; ++i) {
        members[i] = i;
    }
    node::NodeCollection nodes(members, my_id);

    // create a new shared state table with all the members
    mySST sst(nodes);
    sst.count[nodes.my_rank] = 0;
    sst.time[nodes.my_rank] = 0.0;
    sst.update_remote_rows();

    bool ready = false;
    // wait until all counts are 0
    while(ready == false) {
        ready = true;
        for(uint32_t i = 0; i < nodes.num_nodes; ++i) {
	  ready = ready && (sst.count[i] == 0);
        }
    }
    sst.sync_with_members();

    struct timespec start_time;
    volatile bool done = false;

    // the predicate
    auto check_count = [&nodes](const mySST& sst) {
        for(uint32_t i = 0; i < nodes.num_nodes; ++i) {
            if(sst.count[i] < sst.count[nodes.my_rank]) {
                return false;
            }
        }
        return true;
    };

    // trigger. Increments self value
    auto increment_count = [&start_time, &done, &nodes](mySST& sst) {
        ++(sst.count[nodes.my_rank]);
        sst.update_remote_rows(sst.count.get_base_address() - sst.get_base_address(), sizeof(sst.count[0]));
        if(sst.count[nodes.my_rank] == 1000000) {
            // end timer
            struct timespec end_time;
            clock_gettime(CLOCK_REALTIME, &end_time);
            // set the time taken to count
            sst.time[nodes.my_rank] = ((end_time.tv_sec * 1e9 + end_time.tv_nsec) - (start_time.tv_sec * 1e9 + start_time.tv_nsec)) / 1e9;
	    sst.update_remote_rows(sst.time.get_base_address() - sst.get_base_address(), sizeof(sst.time[0]));
            // node rank 0 finds the average by reading all the times taken by remote nodes
            // Anyway, the values will be quite close as the counting is synchronous
            if(nodes.my_rank) {
                bool time_updated = false;
                while(!time_updated) {
                    time_updated = true;
                    for(auto i = 0u; i < nodes.num_nodes; ++i) {
                        time_updated = time_updated && (sst.time[i] != 0.0);
                    }
                }

                double sum = 0.0;
                // compute the average
                for(auto i = 0u; i < nodes.num_nodes; ++i) {
                    sum += sst.time[i];
                }
                ofstream fout;
                fout.open("data_count_write", ofstream::app);
                fout << nodes.num_nodes << " " << sum / nodes.num_nodes << endl;
                fout.close();
	    }
	    sst.sync_with_members();
	    done = true;
        }
    };

    // start timer
    clock_gettime(CLOCK_REALTIME, &start_time);

    // register as a recurring predicate
    sst.predicates.insert(check_count, increment_count, PredicateType::RECURRENT);

    while(!done) {
    }
    return 0;
}
