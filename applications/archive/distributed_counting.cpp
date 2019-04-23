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
        initialize(count);
    }
    SSTField<int> count;
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
    for(uint i = 0; i < num_nodes; ++i) {
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
    sst.update_remote_rows(sst.count.get_base_address() - sst.get_base_address(), sizeof(int));

    bool ready = false;
    // wait until all counts are 0
    while(ready == false) {
        ready = true;
        for(uint32_t i = 0; i < num_nodes; ++i) {
	  ready = ready && (sst.count[i] == 0);
        }
    }
    sst.sync_with_members();

//     struct timespec start_time;

//     // the predicate
//     auto f = [num_nodes](const mySST& sst) {
//         for(uint32_t i = 0; i < num_nodes; ++i) {
//             if(sst.count[i] < sst.count[my_index]) {
//                 return false;
//             }
//         }
//         return true;
//     };

//     // trigger. Increments self value
//     auto g = [&start_time](mySST& sst) {
//         ++(sst.count[my_index]);
//         sst.put((char*)std::addressof(sst.count[0]) - sst.getBaseAddress(), sizeof(int));
//         if(sst.count[my_index] == 1000000) {
//             // end timer
//             struct timespec end_time;
//             clock_gettime(CLOCK_REALTIME, &end_time);
//             // my_time is time taken to count
//             double my_time = ((end_time.tv_sec * 1e9 + end_time.tv_nsec) - (start_time.tv_sec * 1e9 + start_time.tv_nsec)) / 1e9;
//             int my_id = sst.get_local_index();
//             // node 0 finds the average by reading all the times taken by remote nodes
//             // Anyway, the values will be quite close as the counting is synchronous
//             if(my_id == 0) {
//                 int num_nodes = sst.get_num_rows();
//                 resources* res;
//                 double times[num_nodes];
//                 const auto tid = std::this_thread::get_id();
//                 // get id first
//                 uint32_t id = util::polling_data.get_index(tid);
//                 util::polling_data.set_waiting(tid);

//                 // read the other nodes' time
//                 for(int i = 0; i < num_nodes; ++i) {
//                     if(i == my_id) {
//                         times[i] = my_time;
//                     } else {
//                         res = new resources(i, (char*)&my_time, (char*)&times[i], sizeof(double), sizeof(double), my_id < i);
//                         res->post_remote_read(id, sizeof(double));
//                         free(res);
//                     }
//                 }
//                 for(int i = 0; i < num_nodes; ++i) {
//                     util::polling_data.get_completion_entry(tid);
//                 }
//                 util::polling_data.reset_waiting(tid);

//                 double sum = 0.0;
//                 // compute the average
//                 for(int i = 0; i < num_nodes; ++i) {
//                     sum += times[i];
//                 }
//                 ofstream fout;
//                 fout.open("data_count_write", ofstream::app);
//                 fout << num_nodes << " " << sum / num_nodes << endl;
//                 fout.close();

//                 // sync to tell other nodes to exit
//                 for(int i = 0; i < num_nodes; ++i) {
//                     if(i == my_id) {
//                         continue;
//                     }
//                     sync(i);
//                 }
//             } else {
//                 resources* res;
//                 double no_need;
//                 res = new resources(0, (char*)&my_time, (char*)&no_need, sizeof(double), sizeof(double), 0);
//                 sync(0);
//                 free(res);
//             }
// #ifdef USE_VERBS_API
//             verbs_destroy();
// #else
//             lf_destroy();
// #endif
//             exit(0);
//         }
//     };

//     // start timer
//     clock_gettime(CLOCK_REALTIME, &start_time);

//     // register as a recurring predicate
//     sst.predicates.insert(f, g, PredicateType::RECURRENT);

    while(true) {
    }
    return 0;
}
