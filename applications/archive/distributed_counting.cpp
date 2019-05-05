#include <chrono>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <map>
#include <numeric>

#include "conf/conf.hpp"
#include "initialize.hpp"
#include "node/node.hpp"
#include "node/node_collection.hpp"
#include "sst/sst.hpp"
#include "tcp/tcp.hpp"

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

int main(int argc, char* argv[]) {
    if(argc < 2) {
        cout << "Usage: " << argv[0] << " <num_nodes>" << endl;
        return -1;
    }
    // the number of nodes for this test
    const uint32_t num_nodes = std::stoi(argv[1]);

    const std::map<uint32_t, std::pair<tcp::ip_addr_t, tcp::port_t>> ip_addrs_and_ports = initialize(num_nodes);
    uint32_t my_id = derecho::getConfUInt32(CONF_DERECHO_LOCAL_ID);

    rdma::initialize(my_id, ip_addrs_and_ports);

    throw "hello world";

    std::vector<uint32_t> members;
    for(auto p : ip_addrs_and_ports) {
        members.push_back(p.first);
    }
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
    sst.predicates.insert(check_count, increment_count);

    while(!done) {
    }
    return 0;
}
