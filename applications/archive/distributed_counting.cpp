#include <chrono>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <map>
#include <numeric>

#include "node/node.hpp"
#include "node/node_collection.hpp"
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

        std::fill(count.begin(), count.end(), 0);
        std::fill(time.begin(), time.end(), 0.0);
        sync_with_members();
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
    std::iota(members.begin(), members.end(), 0);
    node::NodeCollection nodes(members, my_id);

    // create a new shared state table with all the members
    mySST sst(nodes);

    struct timespec start_time;
    volatile bool done = false;

    // the predicate
    auto check_count = [&nodes](const mySST& sst) {
        for(auto count : sst.count) {
            if(count > sst.count[nodes.my_rank]) {
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
            if(nodes.my_rank == 0) {
                bool time_updated = false;
                while(!time_updated) {
                    time_updated = true;
                    for(auto time : sst.time) {
                        time_updated = time_updated && (time != 0);
                    }
                }

                double sum = std::accumulate(sst.time.begin(), sst.time.end(), 0.0);
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
